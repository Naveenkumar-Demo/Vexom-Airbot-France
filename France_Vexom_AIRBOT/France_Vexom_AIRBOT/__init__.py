from ast import If
from dataclasses import replace
import logging
import json
import re
from typing import Container, List
import azure.functions as func
import os
from azure.core.exceptions import ResourceNotFoundError
from azure.ai.formrecognizer import FormRecognizerClient
from azure.ai.formrecognizer import FormTrainingClient
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import generate_blob_sas
from azure.storage.blob import BlobSasPermissions
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import re
import sys

from pyparsing import And
# //
# import secrets
# from azure.keyvault.secrets import SecretClient
# from azure.identity import ClientSecretCredential

def main(req: func.HttpRequest) -> func.HttpResponse:
    # //
    # credential = ClientSecretCredential("f25493ae-1c98-41d7-8a33-0be75f5fe603","153667e5-3daf-4fd6-b103-d06e411982ee","6lU7Q~rAP.hZ44HqPKLCIYAI4xT~BY0FihSdQ")
    # client = SecretClient(https://airbfvxom-dev-euw-kv-01.vault.azure.net/, credential)
    # secret_bundle=client.get_secret("fr-modelID")
    # model_id=secret_bundle.value
    # //
    endpoint = "https://westeurope.api.cognitive.microsoft.com/"
    key = "bf63a36422854b7d89adb9ef8df13ddf"
    # model_id = "Test_260422"
    model_id ="test"

    form_recognizer_client = FormRecognizerClient(endpoint, AzureKeyCredential(key))
    form_training_client = FormTrainingClient(endpoint, AzureKeyCredential(key))
    #formUrl = https://peenyavce.blob.core.windows.net/input/Esdee_Solutech/Test/1a859770_5111.pdf?sp=racwdyti&st=2022-04-26T06:16:41Z&se=2023-04-26T14:16:41Z&spr=https&sv=2020-08-04&sr=b&sig=NaaN%2Fm%2FYmYhum%2B6o4UhbHqgwbPCswJY4PaIkSJoJYO0%3D
    # New code
    document_analysis_client = DocumentAnalysisClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
    )
  
    exception_type, exception_object, exception_traceback = sys.exc_info()
    logging.info('Python HTTP trigger function processed a request.')
    
    account_key="Tb8ntulL/6yUnOtjVRWEGCC4ToMRe0FVA4xdDPvQ6voGiJS2psl3p0uRkD3syQYw23D2gt5cRjPH1i7WrhGKTA=="
    account_name = "vexombelmarket"
    connection_string = "DefaultEndpointsProtocol=https;AccountName=vexombelmarket;AccountKey=Tb8ntulL/6yUnOtjVRWEGCC4ToMRe0FVA4xdDPvQ6voGiJS2psl3p0uRkD3syQYw23D2gt5cRjPH1i7WrhGKTA==;EndpointSuffix=core.windows.net"

    #blob containers
    target_container_name = "prodinfolder"
    move_file_container_good="good"
    #move_file_container="froutput"
    move_file_container="beloutputmigration"
    move_file_container_bad="bad"
    #sharepoint_container_name = "frsharepoint"
    sharepoint_container_name = "belsharepointmigration"
    blob_spoint_supplier_name="Suppliers.csv"
    blob_spoint_credit_note="CreditNote.csv"
    #blob_spoint_dealer_name="Dealers.csv"
    logcontainer="bellog-migration"
    logcontainer_summary="bellog-summary-migration"
    #logcontainer="frlog"
    #logcontainer_summary="frlog-summary"
    

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(target_container_name)
    container_client1 = blob_service_client.get_container_client(move_file_container)
    container_client_log=blob_service_client.get_container_client(logcontainer)
    container_client_log_summary=blob_service_client.get_container_client(logcontainer_summary)
    container_client2 = blob_service_client.get_container_client(sharepoint_container_name)
    container_client_good=blob_service_client.get_container_client(move_file_container_good)
    container_client_bad=blob_service_client.get_container_client(move_file_container_bad)
    blob_list = container_client.list_blobs()
    blob_list_bad = container_client_bad.list_blobs()
    blob_list_xml = container_client1.list_blobs()
    # blob_list = filter(lambda file1: "pdf" in file1, blob_list)
    # blob_list=list(blob_list)
    csv_file_sharepoint=container_client.list_blobs()
    #blob_new = generate_blob_sas(account_name=account_name,account_key=account_key,container_name="froutput",blob_name="airbfvxomdeveuwstinfra.pdf",permission=BlobSasPermissions(read=True),expiry=datetime.utcnow() + timedelta(hours=1))
    var_ex=""
    var_file_error=""
    var_average_conf_score=""
    line_number="NA"
    result_supplierid=""
    result_dealer_id=""
    var_Supplier_Name=""
    var_InvoiceNo=""
    var_invoicedate=""
    var_VAPNo=""
    var_VATNo=""
    var_Purchase=""
    var_VAT=""
    var_DealerNo=""
    var_filename=""
    var_FreightTotal=""
    var_VAP=""
    var_partno=""
    var_Desc=""
    var_qty=""
    var_totalprice=""
    var_unit_price=""
    #var_city=""
    var_absent_field=""
    #var_Percentage_field =""
    str_xml_header=""
    str_xml_footer=""
    var_vap1=""
    str_xml_invoiceline=""
    str_xml_invoiceline_new=""
    var_supplier_formtype=""
    var_supplier_credit=""
    var_credit="" 
    var_total_input =0
    var_total_bad = 0
    var_total_xml = 0
    var_SAFReduction=""

    #Supplier Sharepoint reference
    blob_sharepoint_reference = generate_blob_sas(account_name=account_name,account_key=account_key,container_name=sharepoint_container_name,blob_name=blob_spoint_supplier_name,permission=BlobSasPermissions(read=True),expiry=datetime.utcnow() + timedelta(hours=1))
    url_supplier = 'https://'+account_name+'.blob.core.windows.net/'+sharepoint_container_name+'/'+blob_spoint_supplier_name+'?'+blob_sharepoint_reference
    df_supplier=pd.read_csv(url_supplier)
    #Credit note sharepoint reference
    blob_sharepoint_creditnote_reference = generate_blob_sas(account_name=account_name,account_key=account_key,container_name=sharepoint_container_name,blob_name=blob_spoint_credit_note,permission=BlobSasPermissions(read=True),expiry=datetime.utcnow() + timedelta(hours=1))
    url_creditnote = 'https://'+account_name+'.blob.core.windows.net/'+sharepoint_container_name+'/'+blob_spoint_credit_note+'?'+blob_sharepoint_creditnote_reference
    df_creditnote=pd.read_csv(url_creditnote)
    
    
    
    for blob in blob_list:          
        blob_name=blob.name.replace(" ","%20")
        blob_new = generate_blob_sas(account_name=account_name,account_key=account_key,container_name=target_container_name,blob_name=blob.name,permission=BlobSasPermissions(read=True),expiry=datetime.utcnow() + timedelta(hours=1))
        url = 'https://'+account_name+'.blob.core.windows.net/'+target_container_name+'/'+blob_name+'?'+blob_new          
        #form_recognizer_client = FormRecognizerClient(cognitive_service_endpoint, AzureKeyCredential(api_key))
        poller = document_analysis_client.begin_analyze_document_from_url(model_id, url)
        result = poller.result()
        Sum_conf=0
        no_fields=len(result.documents[0].fields.items())-1
        for ind_conf in range(no_fields):
            var_ind_conf=list(result.documents[0].fields.values())[ind_conf].confidence
            if(var_ind_conf==None):
                var_ind_conf=0
            Sum_conf=var_ind_conf+Sum_conf
        var_average_conf_score=Sum_conf/(len(result.documents[0].fields.items())-1)
        #for recognized_form in result:
            #var_supplier_formtype=recognized_form.form_type
            # if(var_average_conf_score<0.80):       
            #     break
    
            
        for documents in result.documents:
            try:
                    
                    var_Supplier_Name = result.documents[0].fields["Supplier Name"].value
                    
                    #Credit Note Logic
                    var_credit = result.documents[0].fields["Invoice Type"].value
                    try:
                        result_creditnote=df_creditnote.loc[df_creditnote['Supplier_Name'].str.contains(var_Supplier_Name,regex=True,case=False),'Credit_Note'].values[0]
                        if(var_credit==None):
                            var_credit=""
                        else:
                            if(var_credit==result_creditnote):
                                var_supplier_credit="credit_category"
                                break
                    except:
                        result_creditnote="No Credit Notes for supplier: "+var_Supplier_Name
                    

                    #Field Values
                    var_InvoiceNo = result.documents[0].fields["Invoice No"].value
                    #var_VATNo = result.documents[0].fields["VAT No"].value
                    var_Purchase = result.documents[0].fields["Purchase Invoice Total"].value
                    var_VAT = result.documents[0].fields["VAT"].value
                    #var_DealerNo = result.documents[0].fields["Dealer No"].value
                    var_FreightTotal = result.documents[0].fields["Freight Total"].value
                    
                    #Prefix Logic
                    result_supplier_Prefix=df_supplier.loc[df_supplier['Supplier_Name'].str.contains(var_Supplier_Name,regex=True,case=False),'Prefix'].values[0]
                    

                    #Date Logic
                    var_invoicedate = result.documents[0].fields["Invoice Date"].value.replace(" ","").lower()
                    if(var_invoicedate==None):
                        var_ex="valueisnone"
                        break            
                    if("-" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("-","/")
                    if("." in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace(".","/")
                    if(" " in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace(" ","/")
                    if("k" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("k","c")
                    if("mrt" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("mrt","mar")
                    if("mei" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("mei","may")
                    if("mars" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("mars","march")
                    if("janvier" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("janvier","january")
                    if("février" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("février","February")
                    if("avril" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("avril","april")
                    if("mai" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("mai","may")
                    if("juin" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("juin","june")
                    if("juillet" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("juillet","july")
                    if("août" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("août","august")
                    if("septembre" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("septembre","september")
                    if("octobre" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("octobre","october")
                    if("novembre" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("novembre","november")
                    if("décembre" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("décembre","december")
                    if("//" in var_invoicedate):
                        var_invoicedate=var_invoicedate.replace("//","/")
                    if("BRIDGESTONE" in var_Supplier_Name):
                        if("/" in var_invoicedate):
                            #var_invoicedate="0"+var_invoicedate
                            var_year=var_invoicedate[-2:]
                            var_year="20"+var_year
                            var_daymonth=var_invoicedate[:-2]
                            var_invoicedate=var_daymonth+var_year

                    if("HAZET" in var_Supplier_Name):
                        if(var_invoicedate[1]=="/"):
                            var_invoicedate="0"+var_invoicedate
                            var_year=var_invoicedate[-4:]
                            var_daymonth=var_invoicedate[:-4]
                            var_year="/"+var_year
                            var_invoicedate=var_daymonth+var_year
                    if("HAZET" in var_Supplier_Name):
                        if(var_invoicedate[2]=="/"):
                            var_year=var_invoicedate[-4:]
                            var_daymonth=var_invoicedate[:-4]
                            var_year="/"+var_year
                            var_invoicedate=var_daymonth+var_year
                    format_i = "%d/%m/%y"
                    try:
                        datetime.strptime(var_invoicedate, format_i)
                        var_invoicedate=datetime.strptime(var_invoicedate,"%d/%m/%y").strftime("%Y%m%d")
                        
                    except:
                        format = "%Y/%m/%d"
                        # var_invoicedate=datetime.strptime(var_invoicedate,"%d/%m/%Y").strftime("%Y%m%d")
                        try:
                            datetime.strptime(var_invoicedate, format)
                            var_invoicedate=datetime.strptime(var_invoicedate,"%Y/%m/%d").strftime("%Y%m%d")
                            # print("try "+var_invoicedate)
                        except:
                            format_k="%d/%B/%Y"
                            try:                                        
                                datetime.strptime(var_invoicedate, format_k)
                                var_invoicedate=datetime.strptime(var_invoicedate,"%d/%B/%Y").strftime("%Y%m%d")
                                # print(var_invoicedate)         
                            except:
                                format_j="%d/%b/%Y"
                                try:                                        
                                    datetime.strptime(var_invoicedate, format_j)
                                    var_invoicedate=datetime.strptime(var_invoicedate,"%d/%b/%Y").strftime("%Y%m%d")
                                    print(var_invoicedate)         
                                except:
                                    var_invoicedate=datetime.strptime(var_invoicedate,"%d/%m/%Y").strftime("%Y%m%d")
                                        
                    #No Value for any of the fields,
                    if(var_VAT==None):
                        var_VAT=0
                        var_VAT=str(var_VAT)
                        var_VAT=var_VAT.replace(" ","")
                    else:
                        var_VAT=str(var_VAT)
                        var_VAT=var_VAT.replace(" ","")
                        var_VAT=var_VAT.replace("EUR","")
                        var_VAT=var_VAT.replace("€","")
                        if("." in var_VAT and "," in var_VAT):
                           var_VAT=var_VAT.replace(".","")
                        if("." in var_VAT):
                            var_VAT=var_VAT.replace(".",",")
                    #if(var_Supplier_Name=="CGDPL"):
                        #if(var_FreightTotal is not None):
                            #var_FreightTotal=var_FreightTotal.replace("€","")
                    #else:
                        #var_FreightTotal=var_FreightTotal
                    if(var_FreightTotal==None):
                        var_FreightTotal=0
                        var_FreightTotal=str(var_FreightTotal)
                        var_FreightTotal=var_FreightTotal.replace(" ","")
                        sum_freight=var_FreightTotal
                        sum_freight=str(sum_freight)
                    else :
                        var_FreightTotal = var_FreightTotal.replace(",",".")
                        a = var_FreightTotal.split()
                        sum_freight = 0
                        for x in range(len(a)):
                            sum_freight = sum_freight+float(a[x])
                            sum_freight=round(sum_freight,2)
                        sum_freight = str(sum_freight)
                        sum_freight = sum_freight.replace(".",",")
                        var_FreightTotal=0
                        
                    if(var_InvoiceNo==None):
                        var_ex="valueisnone"
                        break    
                    var_InvoiceNo=var_InvoiceNo.replace(" ","")
                    if(var_Purchase==None):
                        var_ex="valueisnone"
                        break
                    var_Purchase=str(var_Purchase)
                    var_Purchase=var_Purchase.replace(" ","")
                    var_Purchase=var_Purchase.replace("EUR","")
                    var_Purchase=var_Purchase.replace("€","")
                    if("." in var_Purchase and "," in var_Purchase):
                        #if("," in var_Purchase):
                            #var_Purchase=var_Purchase.replace(",","~")
                        #if("." in var_Purchase):
                            #var_Purchase=var_Purchase.replace(".",",")
                        #if("~" in var_Purchase):
                            #var_Purchase = var_Purchase.replace("~","") 
                        var_Purchase=var_Purchase.replace(".","")
                    if("." in var_Purchase):
                        var_Purchase=var_Purchase.replace(".",",")
                    var_Purchase=var_Purchase.replace("*","")

                    #Line Item Details    
                    for field_value in result.documents[0].fields:        
                        if(field_value.__contains__('Line Items')):

                            #Each Line Item Values
                            for x in range(len(result.documents[0].fields[field_value].value)):
                                var_ex=""
                                var_VAPNo = result.documents[0].fields["VAP No"].value
                                if(var_VAPNo==None):
                                    var_ex="valueisnone"
                                    break
                                var_VAPNo=re.sub(r'^.*?V', 'V', var_VAPNo)
                                VAP=''.join(filter(str.isdigit, var_VAPNo))
                                int_VAP=VAP[0:5]
                                var_VAP=f'{np.random.choice([int_VAP]):0>5}'
                                var_totalprice=result.documents[0].fields[field_value].value[x].value.get("Total Part Price")
                                if(var_totalprice==None):
                                    #var_ex="valueisnone"
                                    continue
                                else:
                                    var_partno= result.documents[0].fields[field_value].value[x].value.get("Part No").value
                                    if(var_partno==None):
                                        var_ex="valueisnone"
                                    else:
                                        if(result_supplier_Prefix in var_partno):
                                            var_partno = var_partno
                                        else:
                                            var_partno= result_supplier_Prefix+var_partno
                                        var_partno=var_partno
                                    var_Desc=result.documents[0].fields[field_value].value[x].value.get("Description").value
                                    if("&" in var_Desc):
                                        var_Desc = var_Desc.replace("&"," ")
                                    if(var_Desc==None):
                                        var_ex="valueisnone"
                                    var_qty=result.documents[0].fields[field_value].value[x].value.get("Qty").value
                                    if(" " in var_qty):
                                        list_qty = list(var_qty.split(" "))
                                        var_qty = list_qty[0]
                                    if(var_qty==None):
                                        var_ex="valueisnone"
                                        break
                                    var_unit_price = result.documents[0].fields[field_value].value[x].value.get("Total Part Price").value
                                    var_unit_price = var_unit_price.replace(" ","")
                                    var_unit_price=var_unit_price.replace("€","")
                                    if("." in var_unit_price and "," in var_unit_price):
                                        var_unit_price = var_unit_price.replace(".","")
                                    var_unit_price = var_unit_price.replace(",",".")
                                    var_unit_price = float(var_unit_price)
                                    var_qty = var_qty.replace(",",".")
                                    var_qty = float(var_qty)
                                    var_unit_price = float(var_unit_price/var_qty)
                                    var_unit_price=round(var_unit_price,2)
                                    var_unit_price=str(var_unit_price)
                                    var_qty=str(var_qty)
                                    if("." in var_unit_price):
                                        var_unit_price=var_unit_price.replace(".",",")
                                    if("." in var_qty):
                                        var_qty=var_qty.replace(".",",")
                                    var_unit_price = str(var_unit_price)
                                    var_qty = str(var_qty)
                                    if(var_unit_price==None):
                                        var_ex="valueisnone"
                                    var_totalprice=result.documents[0].fields[field_value].value[x].value.get("Total Part Price").value
                                        
                                    str_xml_invoiceline ="\n<InvoiceLine>\n\t <ArticleInfo>\n\t <BuyersArticleNo>"+var_partno+"</BuyersArticleNo>\n <ItemDescription>"+var_Desc+"</ItemDescription>\n\r </ArticleInfo>\n <InvoicedQuantity>\n\t <Qty>"+var_qty+"</Qty>\n\r </InvoicedQuantity>\n <PriceDetails>\n\t <Price>\n\t <PriceAmountGross>"+var_unit_price+"</PriceAmountGross>\n <PriceUnitBasisQuantity>"+"1"+"</PriceUnitBasisQuantity>\n\r </Price>\n\r </PriceDetails>\n <ReferenceToMessage>\n\t <PurchaseOrderNoLineID>\n\t <PurchaseOrderNo>"+var_VAP+"</PurchaseOrderNo>\n\r </PurchaseOrderNoLineID>\n\r </ReferenceToMessage>\n\r </InvoiceLine>\n"
                                    str_xml_invoiceline_new=str_xml_invoiceline_new + str_xml_invoiceline
                    str_xml_header="<?xml version="+"\"1.0\""+ " encoding="+"\"UTF-8\""+"?>\n<Envelope>\n\t<Header>\n\t<IssueDate>" + var_invoicedate + "</IssueDate>\n <DocumentNo>" + var_InvoiceNo  + "</DocumentNo>\n <Ship-toParty>\n\t <PartyID>"+result_dealer_id+"</PartyID>\n\r </Ship-toParty>\n <SellerParty>\n\t <PartyID>"+result_supplierid+"</PartyID>\n\r </SellerParty>\n <Currencies>\n\t <InvoicingCurrencyCode>EUR</InvoicingCurrencyCode>\n\r </Currencies>\n <Charge>\n\t <SpecialServiceDescriptionCode>FC</SpecialServiceDescriptionCode>\n <InvoicingCurrency>\n\t <Amount>" + sum_freight + "</Amount>\n\r </InvoicingCurrency>\n\r </Charge>\n\r </Header>\n"
                    str_xml_footer="<InvoiceFoot>\n\t<TotalInvoiceAmount>\n\t <InvoicingCurrency>\n\t <Amount>"+var_Purchase+"</Amount>\n\r </InvoicingCurrency>\n\r </TotalInvoiceAmount>\n <TotalTaxAmount>\n\t <InvoicingCurrency>\n\t <Amount>"+var_VAT+"</Amount>\n\r </InvoicingCurrency>\n\r </TotalTaxAmount>\n\r </InvoiceFoot>\n\r</Envelope>"                  
            except Exception as e:
                exception_type, exception_object, exception_traceback = sys.exc_info()
                line_number = exception_traceback.tb_lineno
                line_number=str(line_number)
                # print("I am in except blobk")
                Str_errormessage=str(e)
                var_file_error="filehavingissue"

        # print(average_con_score)
        
        str_xml=str_xml_header + str_xml_invoiceline_new + str_xml_footer
        var_Supplier_Name=str(var_Supplier_Name)
        if(var_average_conf_score>0.80 and var_ex !="valueisnone" and var_file_error !="filehavingissue" and var_supplier_credit!="credit_category"):
            source_blob = (f"https://{account_name}.blob.core.windows.net/{target_container_name}/{blob.name}")
            blob_client = blob_service_client.get_blob_client(container=move_file_container, blob=blob.name+".xml")
            blob_client.create_append_blob()
            # blob_client.create_page_blob
            var_average_conf_score=str(var_average_conf_score)
            copied_blob = blob_service_client.get_blob_client(move_file_container_good, blob.name)
            copied_blob.start_copy_from_url(source_blob)
            blob_client = container_client1.get_blob_client(blob.name+".xml")
            blob_client1 = container_client_log.get_blob_client("Filename_conf_score.txt")
            blob_client1.upload_blob("\n the average confidence score for the file "+ blob.name +" " +var_average_conf_score+" & form type is  "+var_Supplier_Name ,blob_type="AppendBlob")
            # blob_client.upload_blob("<supplier_name>"+blob.name+"</supplier_name>\n\t<VAT>"+average_con_score+"</VAT>",blob_type="AppendBlob")
            # print(str_xml)
            blob_client.upload_blob(str_xml,blob_type="AppendBlob")
            container_client.delete_blob(blob=blob.name)
            today=datetime.now()
            today=str(today)
            blob_client2 = container_client_log_summary.get_blob_client("Summary.txt")
            #last_modified = blob.properties.last_modified
            blob_client2.upload_blob("\n"+blob.name+";" +today+";Good-folder;"+var_Supplier_Name , blob_type="AppendBlob")
            blob_client2.get_blob_properties().last_modified
            # blob_client2.upload_blob(blob.name+";"+today+";Goodfolder;"+ , blob_type="AppendBlob")
            # variable initialization
            var_ex=""
            var_file_error=""
            var_average_conf_score=""
            line_number="NA"
            result_supplierid=""
            result_dealer_id=""
            var_Supplier_Name=""
            var_InvoiceNo=""
            var_invoicedate=""
            var_average_conf_score=""
            var_VAPNo=""
            var_VATNo=""
            var_Purchase=""
            var_VAT=""
            var_DealerNo=""
            var_filename=""
            var_VAT=""
            var_FreightTotal=""
            var_VAP=""
            var_partno=""
            var_Desc=""
            var_qty=""
            var_totalprice=""
            var_unit_price=""
            #var_city=""
            var_absent_field="NA"
            #var_Percentage_field =""
            str_xml_header=""
            str_xml_footer=""
            var_vap1=""
            str_xml_invoiceline=""
            str_xml_invoiceline_new=""
            var_supplier_formtype=""
            var_supplier_credit=""
            var_credit=""
            var_SAFReduction=""
        else:
            if(var_Supplier_Name==None):var_absent_field="Supplier Name is not provided"
            elif(var_VAT==None):var_absent_field="VAT is not provided"
            elif(var_invoicedate==None):var_absent_field="InvoiceDate is not provided "
            elif(var_FreightTotal==None):var_absent_field="FreigthID is not provided "
            elif(var_InvoiceNo==None):var_absent_field="InvoiceNo is not provided "
            elif(var_Purchase==None):var_absent_field="Purchase Invoice Total is not provided "
            elif(var_VAP==None):var_absent_field="VAP No is not provided "
            elif(var_partno==None):var_absent_field="PartNo is not provided "
            elif(var_Desc==None):var_absent_field="Description is not provided "
            elif(var_qty==None):var_absent_field="Quantity is not provided "
            elif(var_totalprice==None):var_absent_field="TotalPrice is not provided "
            elif(var_unit_price==None):var_absent_field="UnitPrice is not provided "
            elif(var_file_error=="filehavingissue"):var_absent_field="other issue "
            elif(var_supplier_credit=="credit_category"):var_absent_field="Creditnote_category "

            source_blob = (f"https://{account_name}.blob.core.windows.net/{target_container_name}/{blob.name}")
            # blob_client = blob_service_client.get_blob_client(container=move_file_container, blob=blob.name+".xml")
            # blob_client.create_append_blob()
            var_average_conf_score=str(var_average_conf_score)
            copied_blob = blob_service_client.get_blob_client(move_file_container_bad, blob.name)
            copied_blob.start_copy_from_url(source_blob)
            # blob_client = container_client1.get_blob_client(blob.name+".xml")
            blob_client1 = container_client_log.get_blob_client("Filename_conf_score.txt")
            blob_client1.upload_blob("\n the average confidence score for the file "+ blob.name +" " +var_average_conf_score+" & form type is sup1"+var_Supplier_Name+ "sup2& reason for failure error1"+ var_absent_field+"error2" ,blob_type="AppendBlob")
            # log file upload
            blob_client_log=container_client_log.get_blob_client("log.txt")
            blob_client_log.upload_blob("\n  file is failed and the avg confidence score is "+ blob.name +" " +var_average_conf_score+" & supplier name is "+var_Supplier_Name+", line no it failed at "+line_number,blob_type="AppendBlob")
            # blob_client.upload_blob("<supplier_name>"+blob.name+"</supplier_name>\n\t<VAT>"+average_con_score+"</VAT>",blob_type="AppendBlob")
            # print(str_xml)
            # blob_client.upload_blob(str_xml,blob_type="AppendBlob")
            container_client.delete_blob(blob=blob.name)
            today=datetime.now()
            today=str(today)
            blob_client2 = container_client_log_summary.get_blob_client("Summary.txt")
            #blob_prop_date = blob_client2.get_blob_properties().last_modified.day
            blob_client2.upload_blob("\n"+blob.name +";" +today+";Bad-folder;"+ var_Supplier_Name ,blob_type="AppendBlob")
            var_ex=""
            var_file_error=""
            var_average_conf_score=""
            line_number="NA"
            result_supplierid=""
            result_dealer_id=""
            var_Supplier_Name=""
            var_InvoiceNo=""
            var_invoicedate=""
            var_VAPNo=""
            var_VATNo=""
            var_Purchase=""
            var_VAT=""
            var_DealerNo=""
            var_filename=""
            var_FreightTotal=""
            var_VAP=""
            var_partno=""
            var_Desc=""
            var_qty=""
            var_totalprice=""
            var_unit_price=""
             #var_city=""
            var_absent_field=""
             #var_Percentage_field =""
            str_xml_header=""
            str_xml_footer=""
            var_vap1=""
            str_xml_invoiceline=""
            str_xml_invoiceline_new=""
            var_supplier_formtype=""
            var_supplier_credit=""
            var_credit="" 
            var_SAFReduction=""

    # for i in blob_list:
    #     var_total_input = var_total_input + 1
    # for j in blob_list_bad:
    #     var_total_bad = var_total_bad + 1
    # for k in blob_list_xml:
    #     var_total_xml = var_total_xml + 1
    # var_total_input=str(var_total_input)
    # var_total_bad=str(var_total_bad)
    # var_total_xml=str(var_total_xml)
    # # container_client_log.delete_blob("Summary.txt")
    # blob_client2 = container_client_log_summary.get_blob_client("Summary.txt")
    # blob_client2.upload_blob("Total no of file received "+var_total_input +"\nTotal file in bad folder "+var_total_bad+"\nTotal xml created is "+ var_total_xml, blob_type="AppendBlob")

    return func.HttpResponse("success")