//
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
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Text.Json;

namespace Summer.Batch.Common.Util
{
    /// <summary>
    /// Serialization helper.
    /// </summary>
    public static class SerializationUtils
    {
        /// <summary>
        /// Serializes an object to a byte array.
        /// </summary>
        /// <param name="obj">The object to serialize.</param>
        /// <returns>A byte array representing the <paramref name="obj"/>.</returns>
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

        /// <summary>
        /// Deserializes a byte array to an object.
        /// </summary>
        /// <typeparam name="T">&nbsp;The type of the object to deserialize to.</typeparam>
        /// <param name="bytes">The byte array to deserialize.</param>
        /// <returns>The deserialized object.</returns>
        public static T Deserialize<T>(this byte[] bytes)
        {
            //return (T) JsonConvert.DeserializeObject<T>(System.Text.Encoding.Unicode.GetString(bytes));
            //return (T) MessagePackSerializer.Deserialize<T>(bytes);
            using (var stream = new MemoryStream(bytes))
            {
                var serializer = new BinaryFormatter();
                return (T) serializer.Deserialize(stream);
            }
        }

        /// <summary>
        /// Serialize an object into an Json string
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static byte[] SerializeObject(this object obj)
        {
            var options = new JsonSerializerOptions
            {
                WriteIndented = true
            };
            return JsonSerializer.SerializeToUtf8Bytes(obj, options);
        }

        /// <summary>
        /// Reconstruct an object from an byte array data
        /// </summary>
        /// <param name="xml"></param>
        /// <returns></returns>
        public static T DeserializeObject<T>(byte[] data) where T : class
        {
            var utf8Reader = new Utf8JsonReader(data);
            return JsonSerializer.Deserialize<T>(ref utf8Reader);
        }

    }
}
