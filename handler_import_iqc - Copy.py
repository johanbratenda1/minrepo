import mail_parsing
import mail_server
import mail_generation
import whitelist
import ion
import time
import utils
import config
import oss_functions
import numpy as np
import kms
    


def handler(event, context):  
    
    utils.log("Script start.")
    
    #Establish connenction with Key Secret Manager in AliCloud.
    secret_manager = kms.SecretManager().connect(context, config.environment)
    
    #Connect and fetch the email raw text from OSS.
    email, filename = oss_functions.get_trigger_file_as_string(context, config.OSS_REGION, kms.SecretManager().get("oss_bucket"), event)
    utils.log("File '%s' has been fetched from OSS bucket" % filename)    

    mail_parser = mail_parsing.ImportIQCCertificateMailParser(email)
    
    try:
    
        """
        
        Control email 
            *Sender whitelisted?
            *Email structure approved?
            *Attributes valid?
        
        """
        
        # Check that sender is a whitelisted supplier
        if not whitelist.is_on_whitelist(config.OSS_REGION, kms.SecretManager().get("oss_bucket"), config.BROKER_WHITELIST_FILENAME, mail_parser.get_sender().lower(), context):
            raise utils.UnknownSenderException("Your email was not processed. Please contact IFSCN Customs team.", "您的电子邮件未被处理。请联系宜家食品贸易（中国）有限公司，关务部门。",email)
        utils.log("Sender is on whitelist.") 
        
        # Check email structure
        is_valid, is_valid_error_message = mail_parser.is_valid()
        if not is_valid:
            raise utils.BrokerInputException("%s See instructions below." % is_valid_error_message, email)
        
        shipment_number = mail_parser.get_shipment_number()
        attachments = mail_parser.get_attachments()
              
        # Control if shipment no exists in M3.
        if not ion.verify_shipment_number(shipment_number):
            raise utils.BrokerInputException("Shipment number provided '%s' cannot be found in the system. Please make sure the shipment number provided is correct." % shipment_number, email)
        

        """

        Get attributes for all docs with the same shipment number in IDM.

        """
        # Generate a list of how many documents with the same shipment number already exist in IDM
        pid_with_same_shipment_number = ion.search_for_one_attribute_get_pid("IFSCN_IQC", "Shipment", shipment_number)
            
        #Store all attributes in list of dict.
        duplicate_shipment_number_attributes = []
        for pid in pid_with_same_shipment_number:
            
            iqc_attributes_template = {"Shipment":"","Certificate_Reference_Number":"","Verified":""}
            
            attributes_dict = ion.retrieve_attributes_from_idm(pid, iqc_attributes_template)
            attributes_dict["pid"]=pid
            duplicate_shipment_number_attributes.append(attributes_dict)
            
        """
        
        Upload documents to IDM.
        
        """
        
        document_list = []
        document_errors  = []
        
        for attachment_name, attachment_data in mail_parser.get_attachments().items():
            
            certificate_reference_number = utils.remove_filetype(attachment_name)
            
            pid_list_with_same_attribtues = ion.search_for_two_attributes_get_pid("IFSCN_IQC","Certificate_Reference_Number",certificate_reference_number,"Shipment",shipment_number)
            
            #Clean IDM, remove duplicated (to ensure we only keep one copy of the combination shipment no + cert ref no)
            if len(pid_list_with_same_attribtues) > 1:
                for pid in pid_list_with_same_attribtues[1:]:
                    utils.log("More than one combinations of shipment number + certificate reference number. This should not occur. Deleting duplicate with PID '%s'" % pid)
                    ion.delete_document(pid)
                                    
            # If combination of shipment + cert ref no exist in IDM -> Update the document. 
            if len(pid_list_with_same_attribtues)>0:
            
                pid_to_update = pid_list_with_same_attribtues[0]
                
                ion.checkin_or_checkout_pid(pid_to_update, "checkout")
                update_successful = ion.update_document(
                    pid_to_update, {
                        "Filename":attachment_name,
                        "base64_string":attachment_data,
                        "Shipment":shipment_number,
                        "Document_type": "IFSCN_IQC",
                        "Certificate_Reference_Number":certificate_reference_number
                    })
                
                
                # Successful update -> add doc to document_list
                if update_successful:
                    
                    updated_pid = ion.search_for_two_attributes_get_pid("IFSCN_IQC","Certificate_Reference_Number",certificate_reference_number,"Shipment",shipment_number)[0]
                    
                    document_list.append({
                        "attachment_name":attachment_name,
                        "data":attachment_data,
                        "certificate_reference_number":certificate_reference_number,
                        "verified":"Yes",
                        "pid":updated_pid
                    })

                    
                # On technical error, append to document_error and break loop
                else:
                    document_errors.append("There was an issue when trying to update document '%s' to the system. Please try again, if the error occur again contact IFSCN." % attachment_name)
                    break
                                
            # If combination of shipment + cert ref no is unique in IDM -> Upload new document. 
            else: 
                
                pid = ion.store_document_in_idm({
                    "Document_type":"IFSCN_IQC",
                    "Shipment":shipment_number,
                    "Certificate_Reference_Number":certificate_reference_number,
                    "Filename": attachment_name,
                    "base64_string":attachment_data,
                    "Verified":"Yes"
                }, checkout_boolean = "false")

                # On technical error, append to document_error and break loop
                if pid is None:
                    document_errors.append("There was an issue when trying to upload document '%s' to the system. Please try again, if the error occur again contact IFSCN." % attachment_name)
                    break
                else: 
                    document_list.append({
                        "attachment_name":attachment_name,
                        "data":attachment_data,
                        "certificate_reference_number":certificate_reference_number,
                        "verified":"Yes",
                        "pid":pid
                    })

                
        
        #if errors occurec during upload or update, 
        if len(document_errors) > 0 and len(document_list) > 0:

            for document in document_list:
                                
                latest_version_no = ion.get_latest_version_no(document["pid"]) 
                if latest_version_no > 1: 
                    ion.revert_pid_to_version(document["pid"], str(latest_version_no-1))
                else:
                    ion.delete_document(document["pid"])
                    
            raise Exception("A technical error occured when trying to upload or update document '%s'. This could be caused by a failed in the API towards IDM. Please, resolve the issue." % document["attachment_name"])
            
        
        """
        
        Send receipt message/error message to CCT and the broker (sender)
        
        """
                
        #if errors, raise exception. 
        if len(document_errors) > 0: 
            raise utils.BrokerInputException("Error(s) occured when trying to upload the document(s): <br> &#8226; %s" % "<br> &#8226;".join(document_errors), email)
            
        utils.log(
            "Sending receipt email with %i approved documents to sender '%s'" % 
            (len(document_list), mail_parser.get_sender())
        )
        
        send_to =  [mail_parser.get_sender()]
        cc = [kms.SecretManager().get("cct_notification_email_address")]
        
        response = mail_generation.generate_import_iqc_receipt_email(
            kms.SecretManager().get("alimail_import_iqc_email_address"), 
            send_to,
            cc,
            shipment_number, 
            document_list,
            duplicate_shipment_number_attributes            
        )
                
        mail_server.send_email(
            kms.SecretManager().get("alimail_import_iqc_email_address"), 
            kms.SecretManager().get("alimail_import_iqc_password"),
            send_to + cc,
            response
        )
    

    
    except utils.ManualException as e:  
        utils.log(e.desc)
        send_to =  [kms.SecretManager().get("ifscn_notification_email_address")]
        cc = [mail_parser.get_sender()]
        response = mail_generation.generate_manual_handling_email(e, send_to, cc, mail_parser.get_subject(), mail_parser.get_body(), kms.SecretManager().get("alimail_import_iqc_email_address"), mail_parser.get_attachments())
        mail_server.send_email(kms.SecretManager().get("alimail_import_iqc_email_address"), kms.SecretManager().get("alimail_import_iqc_password") , send_to+cc, response)

    except utils.BrokerInputException as e:
        utils.log(e.desc)
        send_to =  [mail_parser.get_sender()]
        cc = []
        response = mail_generation.generate_broker_import_iqc_exception_email(e, send_to,cc, kms.SecretManager().get("alimail_import_iqc_email_address"), mail_parser.get_subject())
        mail_server.send_email(kms.SecretManager().get("alimail_import_iqc_email_address"), kms.SecretManager().get("alimail_import_iqc_password") , send_to+cc, response)  

    except utils.UnknownSenderException as e:
        utils.log(e.desc_english)
        send_to =  [mail_parser.get_sender()]
        cc = []
        response = mail_generation.generate_unknown_sender_exception_email(e,send_to,cc, kms.SecretManager().get("alimail_import_iqc_email_address"), mail_parser.get_subject())
        mail_server.send_email(kms.SecretManager().get("alimail_import_iqc_email_address"), kms.SecretManager().get("alimail_import_iqc_password") , send_to+cc, response)   
        
    except Exception as e:
        utils.log(e)
        filename = filename.split("/")[-1]
        oss_functions.delete_file(context, config.OSS_REGION, kms.SecretManager().get("oss_bucket"), "to_process/import_iqc/" + filename)
        oss_functions.store_string_as_file(context, config.OSS_REGION, kms.SecretManager().get("oss_bucket"), email, "technical_errors/import_iqc/" + filename)
        raise

    finally:
        
        # Remove temporary files, if one fail the other should still be attempted to be removed. 
        failed = False
        try:
            oss_functions.delete_file(context, config.OSS_REGION, kms.SecretManager().get("oss_bucket"), filename)
        except:
            failed = True

        if failed:
            raise RuntimeError("Failed to remove object from OSS.")
        
        utils.log("Done\n")
    
    return
