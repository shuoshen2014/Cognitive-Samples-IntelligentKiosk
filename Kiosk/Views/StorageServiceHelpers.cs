// 
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license.
// 
// Microsoft Cognitive Services: http://www.microsoft.com/cognitive
// 
// Microsoft Cognitive Services Github:
// https://github.com/Microsoft/Cognitive
// 
// Copyright (c) Microsoft Corporation
// All rights reserved.
// 
// MIT License:
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED ""AS IS"", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// 

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;

namespace IntelligentKioskSample.Views
{
    internal class StorageServiceHelpers
    {
        private CloudTableClient _tableClient;
        private CloudTable _table;
        private Dictionary<string, List<CustomerEntity>> _map = new Dictionary<string, List<CustomerEntity>>();
        private Object _thisLock = new Object();

        public StorageServiceHelpers()
        {
            Task.Run(DoWork);
        }
        public async Task DoWork()
        {
            Debug.WriteLine("RCCL: Start background thread");

            while (true)
            {
                Dictionary<string, List<CustomerEntity>> processingMap = new Dictionary<string, List<CustomerEntity>>();
                
                lock(_thisLock)
                {
                    foreach (var item in _map.Keys)
                    {
                        processingMap.Add(item, _map[item]);
                    }

                    _map = new Dictionary<string, List<CustomerEntity>>();
                }

                foreach (var emotions in processingMap.Values)
                {
                    //foreach (var entry in emotions)
                    {
                        var selectedEntry = SelectAnEntry(emotions);
                        // Create the TableOperation object that inserts the customer entity.
                        TableOperation insertOperation = TableOperation.Insert(selectedEntry);

                        Debug.WriteLine("RCCL: customerId:{0}, location:{1}, emostionScore: {2}", selectedEntry.PartitionKey, selectedEntry.Location, selectedEntry.EmotionScore);
                        // Execute the insert operation.
                        await _table.ExecuteAsync(insertOperation);
                    }
                }

                // Batch wait interval 10 seconds
                Task.Delay(10000).Wait();
            }
        }

        private CustomerEntity SelectAnEntry(List<CustomerEntity> emotions)
        {
            emotions.Sort((emotion1, emotion2) => (emotion1.EmotionScore > emotion2.EmotionScore) ? -1 : 1);
            Debug.WriteLine("RCCL: --->");
            for (int i = 0; i < emotions.Count; i++) // (var emotion in emotions)
            {
                Debug.WriteLine("RCCL: {0}", emotions[i].EmotionScore);
            }
            Debug.WriteLine("RCCL: <---");

            float floatIndex = emotions.Count * 0.4f;
            int intIndex = (int)floatIndex;
            if (intIndex > 0)
            {
                intIndex -= 1;
            }

            return emotions[intIndex];
        }

        public class CustomerEntity : TableEntity
        {
            public CustomerEntity(string customId)
            {
                this.PartitionKey = customId;
                this.RowKey = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
            }

            public CustomerEntity() { }

            public double EmotionScore { get; set; }

            public string Location { get; set; }
        }

        internal async System.Threading.Tasks.Task AddAsync(string customerId, string location, double emotionScore)
        {
            // Parse the connection string and return a reference to the storage account.
            if (_tableClient == null)
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=storage2018hack;AccountKey=zdPGuU+vEPjYlu/k6ddJQFAvwQWuSxckbWCAHaKKWCtjw7c+Puy03aBZ0iCl1S62/78rhBehmb7Vk213UtQAWg==;EndpointSuffix=core.windows.net");
                _tableClient = storageAccount.CreateCloudTableClient();
                // Create the CloudTable object that represents the "people" table.
                _table = _tableClient.GetTableReference("videoResults");
            }

            // Create a new customer entity.
            CustomerEntity customer1 = new CustomerEntity(customerId);
            customer1.Location = location;
            customer1.EmotionScore = emotionScore;

            lock(_thisLock)
            {
                if (_map.ContainsKey(customerId))
                {
                    _map[customerId].Add(customer1);
                }
                else
                {
                    List<CustomerEntity> entries = new List<CustomerEntity>();
                    entries.Add(customer1);
                    _map[customerId] = entries;
                }
            }
        }
    }
}