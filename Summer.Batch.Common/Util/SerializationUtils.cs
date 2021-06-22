﻿//
//   Copyright 2015 Blu Age Corporation - Plano, Texas
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text.Json;
using System.Xml;
using System.Xml.Serialization;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Summer.Batch.Common.Util
{
    /// <summary>
    /// Serialization helper.
    /// </summary>
    public static class SerializationUtils
    {

      
        private static readonly string assemblyName = "Deserialization:assemblyName";
        //interface Serialize
        //{
        //}

        //interface Deserialize
        //{
        //}
        /// <summary>
        /// Serializes an object to a byte array.
        /// </summary>
        /// <param name = "obj" > The object to serialize.</param>
        /// <returns>A byte array representing the <paramref name = "obj" />.</ returns >
        public static byte[] Serialize(this object obj)
        {
            /*            JsonSerializer s = new JsonSerializer();
                        s.NullValueHandling = NullValueHandling.Ignore;
                        using (var stream = new MemoryStream())
                        using(StreamWriter sw = new StreamWriter(stream))
                        using (JsonWriter writer = new JsonTextWriter(sw))
                        {
                            s.Serialize(writer, obj);
                            return stream.GetBuffer();
                        }*/



            //          return MessagePackSerializer.Serialize(obj, CompositeResolver.Instance);
            /*          return MessagePackSerializer.Serialize(obj, MessagePack.Resolvers.CompositeResolver.Instance);*/

            using (var stream = new MemoryStream())
            {
                var serializer = new BinaryFormatter();
                serializer.Serialize(stream, obj);
                return stream.GetBuffer();
            }
        }

        ///// <summary>
        ///// Deserializes a byte array to an object.
        ///// </summary>
        ///// <typeparam name = "T" > &nbsp; The type of the object to deserialize to.</typeparam>
        ///// <param name = "bytes" > The byte array to deserialize.</param>
        ///// <returns>The deserialized object.</returns>
        //public static T Deserialize<T>(this byte[] bytes)
        //{
        //    //return (T) JsonConvert.DeserializeObject<T>(System.Text.Encoding.Unicode.GetString(bytes));
        //    //return (T) MessagePackSerializer.Deserialize<T>(bytes);
        //    using (var stream = new MemoryStream(bytes))
        //    {
        //        var serializer = new BinaryFormatter();
        //        return (T)serializer.Deserialize(stream);
        //    }
        //}

        public static T Deserialize<T>(this byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                var serializer = new BinaryFormatter();
                serializer.Binder = new DeserializationBinder(GetBinderList());
                return (T)serializer.Deserialize(stream);
            }
        }

        private static List<string> GetBinderList()
        {
            List<string> assemblyList = new List<string>();
            IConfiguration Configuration = GetConfigurationJson();
            if (Configuration == null)
            {
                return assemblyList;
            }
            else
            {
                var list = Configuration.GetSection(assemblyName);

                if (list != null)
                {
                    foreach (var section in list.GetChildren())
                    {
                        assemblyList.Add(section.Value);
                    }
                }
            }

            return assemblyList;

        }
      

        public static IConfiguration GetConfigurationJson()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(System.AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json",
                optional: true,
                reloadOnChange: true);

            return builder.Build();
        }

        public sealed  class DeserializationBinder : SerializationBinder
        {

            public DeserializationBinder(List<string> list)
            {
               
                CustomDeserializeList = list;
            }

            public List<string> CustomDeserializeList { set; get; }
            
            private static readonly List<string> SummerBatchCore = new List<string>() { "Summer.Batch.Common", "Summer.Batch.Core", "Summer.Batch.Data", "Summer.Batch.Extra", "Summer.Batch.Infrastructure","mscorlib" };
            public override Type BindToType(string assemblyName, string typeName)
            {
                Type typeToDeserialize = null;
                Assembly currentAssembly = Assembly.Load(assemblyName);
 
                //Get List of Class Name
                string Name = currentAssembly.GetName().Name;
                if (SummerBatchCore.Contains(Name) || (CustomDeserializeList.Count != 0 && CustomDeserializeList.Any(name => Name.StartsWith(name))))
                {
                    //The following line of code returns the type.
                    typeToDeserialize = Type.GetType(String.Format("{0}, {1}",typeName, Name));
                }
                else
                {
                    throw new SerializationException("Failed to deserialize. Please create appsettings.json and add assembly name into assembly section of Deserialization.");
                }

                return typeToDeserialize;
            }
        }

    }
}
