﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using cosmosdbquery;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Cosmos;
using Microsoft.ServiceBus.Messaging;
using System.IO;
using Microsoft.ServiceBus;
using Microsoft.Azure.Services.AppAuthentication;
using System.Net.Http;
using System.Net.Http.Headers;

namespace readingmsgs
{
    public class Key
    {
        public string name = "EmailId";
        public string datatype = "STRING";
        public string value { get; set; }
    }

    public class Attribute
    {
        public string key { get; set; }
        public string datatype = "STRING";
        public string value { get; set; }
    }

    public class signal
    {
        public string impactedEntity = "Contact";
        public string signalType = "EmailValidationStatusChange";
        public string action = "Update";
        public List<Key> keys { get; set; }
        public List<Attribute> attributes { get; set; }
        public object headers { get; set; }
        public string correlationId { get; set; }
        public string originatingSystem = "Marketo";
        public DateTime originatingSystemDate { get; set; }
        public string internalProcessor = "CVES";
        public DateTime internalProcessingDate { get; set; }
        public DateTime signalAcquisitionDate { get; set; }
    }
    public class Program
    {
        const string ServiceBusConnectionString = "Endpoint=sb://getmarketoactivities.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=teUOgtLtNhQMuXuhiH5o0fmTD4a3GX7dj99eudc/NBk=";
        const string TopicName = "marketo_topic";
        const string SubscriptionName = "Marketo_subscription";
        static Microsoft.ServiceBus.Messaging.SubscriptionClient subscriptionClient = Microsoft.ServiceBus.Messaging.SubscriptionClient.CreateFromConnectionString(ServiceBusConnectionString, TopicName, SubscriptionName);
        SubscriptionDescription sd = new SubscriptionDescription(TopicName,SubscriptionName)
        {
            EnableBatchedOperations = true
        };
        
        public static readonly string EndpointUri = "https://learn-cos.documents.azure.com:443/";

        public static readonly string PrimaryKey = "PS7eKnpOSJ2aInEGeJbiqk4V8Vab7vZx9OFYUUajjDlhLWpnsgmwuASX0sJ6UAibjy9YYqJDWLGiexqyYJ2uaQ==";
        public static CosmosClient azurecosmosclient;
        public static Database azuredb;
        public static Container azurecontainer;
     static   List<getmainmail> emailist = new List<getmainmail>();
       static List<string> statuschange = new List<string>();

        public static async Task GetRef()
        {
            azurecosmosclient = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions()
            {
                ConnectionMode = ConnectionMode.Gateway
            });
           azuredb = azurecosmosclient.GetDatabase("mydb");
            azurecontainer = azuredb.GetContainer("collection1");
     
        }
        public static async Task AddItemstoMainContainer(getmainmail obj)
        {
            try
            {
                ItemResponse<getmainmail> Response = await azurecontainer.UpsertItemAsync<getmainmail>(obj);
                Console.WriteLine("updated item in database");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        public static async Task getemailobject(string id)
        {
            emailist.Clear();
            var sqlQueryText = $"SELECT * FROM c WHERE c.id =\"{id}\" ";
            //  Console.WriteLine(sqlQueryText);
            // Console.ReadKey();
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<getmainmail> queryResultSetIterator = azurecontainer.GetItemQueryIterator<getmainmail>(queryDefinition);
            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<getmainmail> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (getmainmail item in currentResultSet)
                {

                    emailist.Add(item);
                     }
            }

        }
        public static async Task<string> ReadItemsFromMainCollection(string id, PartitionKey p)
        {
            try
            {
                ItemResponse<getmainmail> Responseitem = await azurecontainer.ReadItemAsync<getmainmail>(id, p);
                var itembody = Responseitem.Resource;
                var statusmail = itembody.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusNbr;
                return statusmail.ToString();
            }
            catch(Exception e)
            {
                return ("item not found");
            }
        }
        
        public static async Task SendSignalviaHttpAsync(signal signalObject)
        {
            var token = new AzureServiceTokenProvider("RunAs=App;AppId=aa0c3919-10cc-41aa-b236-35329c72ce95;TenantId=72f988bf-86f1-41af-91ab-2d7cd011db47;CertificateThumbprint=624225424959582dc202a153b69aa7f85c90c57b;CertificateStoreLocation=LocalMachine");
            var requiredtoken = await token.GetAccessTokenAsync("https://activitystore-ppe.trafficmanager.net");
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", requiredtoken);
            var json = JsonConvert.SerializeObject(signalObject);
            var data = new StringContent(json, Encoding.UTF8, "application/json");

            var signalurl = "https://activitystore-ppe.trafficmanager.net/signalacquisition-dev/api/v1/signal";

            var response = await client.PostAsync(signalurl, data);

            string result = await response.Content.ReadAsStringAsync();
            Console.WriteLine(result);
        }
        public static async Task Main(string[] args)
        {
           // Program programobject = new Program();
            
         await GetRef();
            IEnumerable<BrokeredMessage> messageList = await subscriptionClient.ReceiveBatchAsync(2);
            foreach (var message in messageList)
                processingmessage(message);
           
            Console.ReadKey();
         
            await subscriptionClient.CloseAsync();
        }

        public static async void processingmessage(BrokeredMessage message)
        {
            Dictionary<string, string> dict = new Dictionary<string, string>();
            dict.Add("200", "EMAIL_VALID");
            dict.Add("210", "DOMAIN_EXISTS");
            dict.Add("220", "RETRY");
            dict.Add("250", "EMAIL_EXISTS_BUT_SPAM");
            dict.Add("260", "DOMAIN_EXISTS_BUT_SPAM");
            dict.Add("270", "RETRY");
            dict.Add("300", "EMAIL_NOT_VALID");
            dict.Add("310", "DOMAIN_EXISTS_BUT_SPAM");
           
            Stream stream = message.GetBody<Stream>();
            StreamReader reader = new StreamReader(stream);
            string messagestring = reader.ReadToEnd();
            Console.WriteLine(messagestring);
            JObject json = JObject.Parse(messagestring);
            string emailstatus = (string)json["activityType"];
            string mailId = (string)json["mailid"];
            string codeofstatus;
            getmainmail mainmail = new getmainmail();
            if (emailstatus == "Open Email" || emailstatus == "Click Email" || emailstatus == "Email Delivered")
            {
                emailstatus = "Email Valid";
                codeofstatus = "200";
                mainmail = JsonConvert.DeserializeObject<getmainmail>(messagestring);

                VerifyEmailResponse verifyemailresponse = new VerifyEmailResponse();
                VerifyEmailResult verifyemailres = new VerifyEmailResult();
                verifyemailresponse.VerifyEmailResult = verifyemailres;
                ServiceResult servresult = new ServiceResult();
                ServiceStatus servstatus = new ServiceStatus();
                Hygiene hygieneobj = new Hygiene();
                Email mailobject = new Email();
                Reason Reasonobj = new Reason();
                verifyemailres.ServiceStatus = servstatus;
                verifyemailres.ServiceResult = servresult;
                servresult.Reason = Reasonobj;
                servresult.Email = mailobject;
                servresult.Hygiene = hygieneobj;
                mainmail.VerifyEmailResponse = verifyemailresponse;            
                mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete = mailId;
                mainmail.id = mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete;
                mainmail.partitionKey = mainmail.id.Substring(0, 2);
                               
                var statusmail = await ReadItemsFromMainCollection(mainmail.id, new PartitionKey(mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete.Substring(0, 2)));
                mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusNbr = long.Parse(codeofstatus);
                mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusDescription = emailstatus;
                mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Timestamp = DateTime.UtcNow;
                if (statusmail != codeofstatus && statusmail != "item not found")
                {
                    signal SignalObject = new signal();
                    List<Attribute> attributelist = new List<Attribute>();
                    List<Key> keylist = new List<Key>();
                    Attribute attributeobjectnew = new Attribute();
                    Attribute attributeobjectold = new Attribute();
                    Guid g = Guid.NewGuid();
                    Key keyobject = new Key();
                    keyobject.value = mainmail.id;
                    keylist.Add(keyobject);
                    SignalObject.keys =keylist;
                    SignalObject.correlationId = g.ToString();
                    SignalObject.originatingSystemDate = (DateTime)json["EventProcessedUtcTime"];

                    attributeobjectnew.key = "EmailMatchType.new";
                    attributeobjectnew.value = dict[codeofstatus];
                    attributeobjectold.key = "EmailMatchType.old";
                    attributeobjectold.value = dict[statusmail];
                    attributelist.Add(attributeobjectold);
                    attributelist.Add(attributeobjectnew);
                    SignalObject.attributes = attributelist;
                    SignalObject.internalProcessingDate = DateTime.UtcNow;
                    await SendSignalviaHttpAsync(SignalObject);
                   }
                    await AddItemstoMainContainer(mainmail);
                message.Complete();
                Console.WriteLine("message deleted");
            }           
            

            else
            {
                mainmail = JsonConvert.DeserializeObject<getmainmail>(messagestring);

                VerifyEmailResponse verifyemailresponse = new VerifyEmailResponse();
                VerifyEmailResult verifyemailres = new VerifyEmailResult();
                verifyemailresponse.VerifyEmailResult = verifyemailres;
                ServiceResult servresult = new ServiceResult();
                ServiceStatus servstatus = new ServiceStatus();
                Hygiene hygieneobj = new Hygiene();
                Email mailobject = new Email();
                Reason Reasonobj = new Reason();
                verifyemailres.ServiceStatus = servstatus;
                verifyemailres.ServiceResult = servresult;
                servresult.Reason = Reasonobj;
                servresult.Email = mailobject;
                servresult.Hygiene = hygieneobj;
                mainmail.VerifyEmailResponse = verifyemailresponse;
              //  mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusDescription = emailstatus;
               
                mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete = mailId;
                mainmail.id = mailId;
                mainmail.partitionKey = mainmail.id.Substring(0, 2);
                var statusmail = await ReadItemsFromMainCollection(mainmail.id, new PartitionKey(mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete.Substring(0, 2)));
                if (statusmail == "item not found")
                {
                    mainmail.BounceCount++;
                    await AddItemstoMainContainer(mainmail);
                }
                else if (statusmail != "300" && statusmail != "item not found")
                {
                    mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusNbr = long.Parse(statusmail);
                    await getemailobject(mainmail.id);
                     mainmail = emailist[0];
                    emailist[0].BounceCount++;
                    await AddItemstoMainContainer(emailist[0]);
                    Console.WriteLine(emailist[0].BounceCount);

                    if (mainmail.BounceCount == 5)
                    {
                        emailstatus = "Email Not Valid";
                        codeofstatus = "300";                     

                        if (statusmail != codeofstatus && statusmail != "item not found" && statusmail != "300")
                        {
                            mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusDescription = emailstatus;
                            mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusNbr = long.Parse(codeofstatus);
                            mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Timestamp = DateTime.UtcNow;
                            if (statusmail != "0")
                            {
                                signal SignalObject = new signal();
                                List<Attribute> attributelist = new List<Attribute>();
                                Attribute attributeobjectnew = new Attribute();
                                Attribute attributeobjectold = new Attribute();
                                List<Key> keylist = new List<Key>();
                                Guid g = Guid.NewGuid();
                                Key keyobject = new Key();
                                keyobject.value = mainmail.id;
                                keylist.Add(keyobject);
                                SignalObject.keys = keylist;
                                SignalObject.correlationId = g.ToString();
                                SignalObject.originatingSystemDate = (DateTime)json["EventProcessedUtcTime"];

                                attributeobjectnew.key = "EmailMatchType.new";
                                attributeobjectnew.value = dict[codeofstatus];
                                attributeobjectold.key = "EmailMatchType.old";
                                attributeobjectold.value = dict[statusmail];
                                attributelist.Add(attributeobjectold);
                                attributelist.Add(attributeobjectnew);
                                SignalObject.attributes = attributelist;
                                SignalObject.internalProcessingDate = DateTime.UtcNow;

                                 await SendSignalviaHttpAsync(SignalObject);
                            }
                            await AddItemstoMainContainer(mainmail);
                        }
                    }
                }
                    
                    message.Complete();
                    Console.WriteLine("message deleted");
                }
        }
        }
    }

