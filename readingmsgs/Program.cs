using System;
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
        public string signalType = "EmailValidationDataUpdate";
        public string action = "Update";
        public Key keys { get; set; }
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
                Console.WriteLine("updated item in database with id: {0} \n", Response.Resource.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
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
        public static async Task Delete(string id,PartitionKey p)
        {
         
            // Delete an item. Note we must provide the partition key value and id of the item to delete
            ItemResponse<getmainmail> deleteResponse = await azurecontainer.DeleteItemAsync<getmainmail>(id,p);
            Console.WriteLine("Deleted item");
        }
        public static async Task SendSignalviaHttpAsync(signal signalObject)
        {
            var token = new AzureServiceTokenProvider("RunAs=App;AppId=aa0c3919-10cc-41aa-b236-35329c72ce95;TenantId=72f988bf-86f1-41af-91ab-2d7cd011db47;CertificateThumbprint=624225424959582dc202a153b69aa7f85c90c57b;CertificateStoreLocation=CurrentUser");
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
            Program programobject = new Program();
         await GetRef();
            IEnumerable<BrokeredMessage> messageList = await subscriptionClient.ReceiveBatchAsync(2);
            foreach (var message in messageList)
                processingmessage(message);
           
            Console.ReadKey();
         
            await subscriptionClient.CloseAsync();
        }

        public static async void processingmessage(BrokeredMessage message)
        {
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
                mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusDescription = emailstatus;

                mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete = mailId;
                mainmail.Id = mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete;
                signal SignalObject = new signal();
                List<Attribute> attributelist = new List<Attribute>();
                Attribute attributeobjectnew = new Attribute();
                Attribute attributeobjectold = new Attribute();
                var statusmail = await ReadItemsFromMainCollection(mainmail.Id, new PartitionKey(mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete.Substring(0, 2)));
                if (statusmail != codeofstatus && statusmail != "item not found")
                {
                    statuschange.Add(mainmail.Id);
                    Guid g = Guid.NewGuid();
                    SignalObject.correlationId = g.ToString();
                    SignalObject.originatingSystemDate = (DateTime)json["EventProcessedUtcTime"];
                    SignalObject.keys.value = mainmail.Id;
                    attributeobjectnew.key = "StatusCode.new";
                    attributeobjectnew.value = codeofstatus;
                    attributeobjectold.key = "StatusCode.old";
                    attributeobjectold.value = statusmail;
                    SignalObject.attributes = attributelist;
                    SignalObject.internalProcessingDate = DateTime.UtcNow;
                    SendSignalviaHttpAsync(SignalObject);
                    await Delete(mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete, new PartitionKey(mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete.Substring(0, 2)));
                    await AddItemstoMainContainer(mainmail);
                }
                else if (statusmail == "item not found")
                    await AddItemstoMainContainer(mainmail);
                foreach (var item in statuschange)
                    Console.WriteLine(item);
                message.Complete();
                Console.WriteLine("message deleted");
            }

            else
            {
                mainmail.BounceCount++;
                if (mainmail.BounceCount == 5)
                {
                    emailstatus = "Email Not Valid";
                    codeofstatus = "300";
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
                    mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusDescription = emailstatus;

                    mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete = mailId;
                    mainmail.Id = mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete;
                    signal SignalObject = new signal();
                    List<Attribute> attributelist = new List<Attribute>();
                    Attribute attributeobjectnew = new Attribute();
                    Attribute attributeobjectold = new Attribute();
                    var statusmail = await ReadItemsFromMainCollection(mainmail.Id, new PartitionKey(mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete.Substring(0, 2)));
                    if (statusmail != codeofstatus && statusmail != "item not found")
                    {
                        statuschange.Add(mainmail.Id);
                        Guid g = Guid.NewGuid();
                        SignalObject.correlationId = g.ToString();
                        SignalObject.originatingSystemDate = (DateTime)json["EventProcessedUtcTime"];
                        SignalObject.keys.value = mainmail.Id;
                        attributeobjectnew.key = "StatusCode.new";
                        attributeobjectnew.value = codeofstatus;
                        attributeobjectold.key = "StatusCode.old";
                        attributeobjectold.value = statusmail;
                        SignalObject.attributes = attributelist;
                        SignalObject.internalProcessingDate = DateTime.UtcNow;
                        SendSignalviaHttpAsync(SignalObject);
                        await Delete(mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete, new PartitionKey(mainmail.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete.Substring(0, 2)));
                        await AddItemstoMainContainer(mainmail);
                    }
                    else if (statusmail == "item not found")
                        await AddItemstoMainContainer(mainmail);
                    foreach (var item in statuschange)
                        Console.WriteLine(item);
                    message.Complete();
                    Console.WriteLine("message deleted");
                }
            }
        }
        }
    }

