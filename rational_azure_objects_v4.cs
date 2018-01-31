using System;
using System.Configuration;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Queue;

using RationalZone.v4;

/// <summary>
/// RationalZone.v4.AzureObjects module for an easy to use Azure Table and Queue objects. Aim is to make
/// it easy to create custom data types and add type dependent logic(e.g.load/save handling) and helper
/// functions(e.g.add a Dictionary of elements to a table)
/// </summary>
namespace RationalZone.v4.AzureObjects
{
    /// <summary>
    /// Common interface for Azure objects, both table and queue
    /// </summary>
    public interface IAzureObject
    {
        /// <summary>
        /// Array of properties defined by an IAzureObject instance
        /// </summary>
        PropertyInfo[] DeclaredProperties { get; }
        /// <summary>
        /// Optional default values that should be used for undefined properties.
        /// </summary>
        NameValueCollection DefaultValues { get; }
    }

    public class AzureHelpers
    {
        public static PropertyInfo[] _getDeclaredProperties(Type t)
        {
            List<PropertyInfo> result = new List<PropertyInfo>();
            result.Add(t.GetProperty("id"));
            PropertyInfo[] declared_properties= t.GetProperties(BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
            foreach (PropertyInfo pi in declared_properties)
            {
                Attribute[] attributes = Attribute.GetCustomAttributes(pi, typeof(IgnorePropertyAttribute), true);
                if (attributes == null || attributes.Length == 0)
                    result.Add(pi);
            }
            return result.ToArray();
        }

        public static string _getProperty(IAzureObject obj, PropertyInfo property)
        {
            if (obj != null && property != null)
            {
                object result = property.GetValue(obj, null);
                if (result != null)
                    return result.ToString();
                else
                    return obj.DefaultValues[property.Name];
            }
            else
            {
                return null;
            }
        }

        public static string _getProperty(IAzureObject obj, string key)
        {
            return _getProperty(obj, obj.GetType().GetProperty(key));
        }

        public static void _setProperty(IAzureObject obj, PropertyInfo property, string value)
        {
            if (obj != null && property != null)
            {
                if (value != null)
                    property.SetValue(obj, (object)value, null);
                else
                    property.SetValue(obj, obj.DefaultValues[property.Name], null);
            }
        }

        public static void _setProperty(IAzureObject obj, string key, string value)
        {
            _setProperty(obj, obj.GetType().GetProperty(key), value);
        }

        public static void _addCollection(IAzureObject obj, NameValueCollection collection)
        {
            if (collection != null)
            {
                foreach (PropertyInfo property in obj.DeclaredProperties)
                {
                    string value = collection[property.Name];
                    if (value != null)
                        property.SetValue(obj, value, null);
                }
            }
        }
        public static string _exportAsIni(IAzureObject obj)
        {
            StringBuilder b = new StringBuilder();
            foreach (PropertyInfo property in obj.DeclaredProperties)
                b.Append(property.Name).Append("=").Append(property.GetValue(obj, null)).AppendLine();

            return b.ToString();
        }

        public static string _exportAsJson(IAzureObject obj, string[] keys)
        {
            StringBuilder b = new StringBuilder();
            foreach (string key in keys)
                b.Append(",\"").Append(key).Append("\":\"").Append(_getProperty(obj, key)).Append("\"");

            b.Remove(0, 1).Insert(0, "{").Append("}"); // remove first comma and add brackets
            return b.ToString();
        }

        public static string _exportAsJson(IAzureObject obj)
        {
            StringBuilder b = new StringBuilder();
            foreach (PropertyInfo property in obj.DeclaredProperties)
                b.Append(",\"").Append(property.Name).Append("\":\"").Append(property.GetValue(obj, null)).Append("\"");

            b.Remove(0, 1).Insert(0, "{").Append("}"); // remove first comma and add brackets
            return b.ToString();
        }

        public static string _exportAsHtml(IAzureObject obj, string name_tag="", string value_tag = "", string value_delimeter = "=", string property_delimeter = "<br />")
        {
            StringBuilder b = new StringBuilder();
            foreach (PropertyInfo property in obj.DeclaredProperties)
                b.Append(name_tag).Append(property.Name).Append(name_tag.Replace("<", "</")).Append(value_delimeter).Append(value_tag).Append(property.GetValue(obj, null)).Append(value_tag.Replace("<", "</")).Append(property_delimeter).AppendLine();

            return b.ToString();
        }

        public static Dictionary<string, string> _exportAsDictionary(IAzureObject obj)
        {
            Dictionary<string, string> result = new Dictionary<string, string>();
            foreach (PropertyInfo property in obj.DeclaredProperties)
                result[property.Name] = property.GetValue(obj, null) as string;

            return result;
        }

        public static void _copyFromTable(IAzureObject obj, DynamicTableEntity entity)
        {
            if (entity != null)
            {
                foreach (PropertyInfo property in obj.DeclaredProperties)
                {
                    string name = property.Name;
                    if (entity.Properties.ContainsKey(name))
                        _setProperty(obj, property, entity.Properties[name].StringValue);
                    else
                        _setProperty(obj, property, null); // there might be default value to be assigned
                }
            }
        }
        public static void _copyFromHttp(IAzureObject obj, HttpRequest request)
        {
            if (request != null)
                foreach (PropertyInfo property in obj.DeclaredProperties)
                    _setProperty(obj, property, request[property.Name]);
        }
        public static void _copyFromJson(IAzureObject obj, string json_data)
        {
            if (json_data != null)
                foreach (PropertyInfo property in obj.DeclaredProperties)
                    _setProperty(obj, property, Utils.stringFindJsonValue(json_data, property.Name));
        }
        public static void _copyFromIni(IAzureObject obj, string ini_data)
        {
            if (ini_data != null)
            {
                string[] lines = ini_data.Split(new[] { "\r\n", "\r", "\n" }, StringSplitOptions.None);
                foreach (string line in lines)
                {
                    int separator = line.IndexOf('=');
                    if (separator > 0)
                    {
                        _setProperty(obj, line.Substring(0, separator), line.Substring(separator+1));
                    }                    
                }
            }
        }
        public static void _copyFromDictionary(IAzureObject obj, Dictionary<string, string> data)
        {
            if (data != null)
                foreach (KeyValuePair<string, string> kvp in data)
                    _setProperty(obj, kvp.Key, kvp.Value);
        }
    }

    /// <summary>
    /// Base class for Azure table objects
    /// </summary>
    /// <seealso cref="Microsoft.WindowsAzure.Storage.Table.TableEntity" />
    /// <seealso cref="RationalZone.v4.AzureObjects.IAzureObject" />
    public abstract class TableObject : TableEntity, IAzureObject
    {
		private static DateTime _default_datetime = new DateTime(1900, 1, 1);

        protected static CloudStorageAccount _account = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["dotnet-azure-objects.connectionstring"]);
        protected static CloudTableClient _client = _account.CreateCloudTableClient();
        protected static Dictionary<string, CloudTable> _tables = new Dictionary<string, CloudTable>();

        protected CloudTable _table = null;
        protected PropertyInfo[] _properties = null;

        [IgnoreProperty]
        public PropertyInfo[] DeclaredProperties { get { return _properties; } }
        [IgnoreProperty]
        public abstract NameValueCollection DefaultValues { get; }

        public string error { get; set; }

		[IgnoreProperty]
		public DateTime load_time { get; set; }
        [IgnoreProperty]
        public string id {
            get { return RowKey; }
            set { RowKey = value; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableObject"/> class.
        /// </summary>
        public TableObject() {
            _properties = AzureHelpers._getDeclaredProperties(this.GetType());
            _table = _getTable(this.GetType().Name);
            error = "";
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableObject"/> class with a custom PartitionKey.
        /// </summary>
        /// <param name="_partition">The partition.</param>
        /// <param name="_id">The identifier.</param>
        public TableObject(string _partition, string _id) : this()
        {
            PartitionKey = _partition;
            RowKey = _id;
			load_time = _default_datetime; // azure can't store DateTime.minValue
        }
        /// <summary>
        /// Initializes a new instance of the <see cref="TableObject"/> class with a Partition key as a prefix of the id.
        /// </summary>
        /// <param name="_id">The identifier.</param>
        /// <param name="partition_prefix">The partition prefix.</param>
        public TableObject(string _id, int partition_prefix = 2) : this()
        {
            PartitionKey = _id.Substring(0, Math.Min(partition_prefix, _id.Length));
            RowKey = _id;
            load_time = _default_datetime; // azure can't store DateTime.minValue
        }
        /// <summary>
        /// Determines whether this instance is loaded.
        /// </summary>
        /// <returns>
        ///   <c>true</c> if this instance is loaded; otherwise, <c>false</c>.
        /// </returns>
        public bool isLoaded()
		{
			return isLoadedSince(_default_datetime);
		}
        /// <summary>
        /// Determines whether [is loaded since] [the specified time].
        /// </summary>
        /// <param name="time">The time.</param>
        /// <returns>
        ///   <c>true</c> if [is loaded since] [the specified time]; otherwise, <c>false</c>.
        /// </returns>
        public bool isLoadedSince(DateTime time) 
		{
			return load_time > time;
		}
		protected static CloudTable _getTable(string table_name)
		{
            table_name = table_name.ToLower();
            if (_tables.ContainsKey(table_name))
                return _tables[table_name];

            CloudTable table = _client.GetTableReference(table_name);
            table.CreateIfNotExists();
            _tables[table_name] = table;
            return table;
        }

        /// <summary>
        /// Gets the property by key name.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public string getProperty(string key)
        {
            return AzureHelpers._getProperty(this, key);
        }
        /// <summary>
        /// Gets the property by PropertyInfo.
        /// </summary>
        /// <param name="property">The property.</param>
        /// <returns></returns>
        public string getProperty(PropertyInfo property)
        {
            return AzureHelpers._getProperty(this, property);
        }
        /// <summary>
        /// Sets the property by key name.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public void setProperty(string key, string value)
        {
            AzureHelpers._setProperty(this, key, value);
        }
        /// <summary>
        /// Sets the property by PropertyInfo.
        /// </summary>
        /// <param name="property">The property.</param>
        /// <param name="value">The value.</param>
        public void setProperty(PropertyInfo property, string value)
        {
            AzureHelpers._setProperty(this, property, value);
        }
        /// <summary>
        /// Adds properties from a NameValueCollection.
        /// </summary>
        /// <param name="collection">The collection.</param>
        public void addCollection(NameValueCollection collection)
        {
            AzureHelpers._addCollection(this, collection);
        }
        /// <summary>
        /// Exports properties as json.
        /// </summary>
        /// <param name="keys">The keys.</param>
        /// <returns></returns>
        public string exportAsJson(string[] keys)
        {
            return AzureHelpers._exportAsJson(this, keys);
        }
        /// <summary>
        /// Exports properties as json.
        /// </summary>
        /// <returns></returns>
        public string exportAsJson()
        {
            return AzureHelpers._exportAsJson(this);
        }
        /// <summary>
        /// Exports properties as dictionary.
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, string> exportAsDictionary()
        {
            return AzureHelpers._exportAsDictionary(this);
        }
        /// <summary>
        /// Exports properties as HTML.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="name_tag">The name tag e.g. '<td>'.</param>
        /// <param name="value_tag">The value tag e.g. '<td>'.</param>
        /// <param name="value_delimeter">The value delimeter e.g. '='.</param>
        /// <param name="property_delimeter">The property delimeter e.g. '<br />'.</param>
        /// <returns></returns>
        public string exportAsHtml(string name_tag = "", string value_tag = "", string value_delimeter = "=", string property_delimeter = "<br />")
        {
            return AzureHelpers._exportAsHtml(this, name_tag, value_tag, value_delimeter, property_delimeter);
        }
        /// <summary>
        /// Copies properties from a DynamicTableEntity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        public void copyFromTable(DynamicTableEntity entity)
        {
            if (entity != null)
            {
                AzureHelpers._copyFromTable(this, entity);
                this.PartitionKey = entity.PartitionKey;
                this.RowKey = entity.RowKey;
                this.ETag = entity.ETag;
                this.Timestamp = entity.Timestamp;
            }
        }
        /// <summary>
        /// Copies properties from a HTTP request.
        /// </summary>
        /// <param name="request">The request.</param>
        public void copyFromHttp(HttpRequest request)
        {
            AzureHelpers._copyFromHttp(this, request);
        }
        /// <summary>
        /// Copies properties from json-data.
        /// </summary>
        /// <param name="json_data">The json data.</param>
        public void copyFromJson(string json_data)
        {
            AzureHelpers._copyFromJson(this, json_data);
        }
        /// <summary>
        /// Copies properties from ini-data.
        /// </summary>
        /// <param name="ini_data">The ini data.</param>
        public void copyFromIni(string ini_data)
        {
            AzureHelpers._copyFromIni(this, ini_data);
        }
        /// <summary>
        /// Copies properties from a string-dictionary.
        /// </summary>
        /// <param name="data">The data.</param>
        public void copyFromDictionary(Dictionary<string, string> data)
        {
            AzureHelpers._copyFromDictionary(this, data);
        }

        protected string _getQueryFilter(string query_comparison = QueryComparisons.Equal, string table_operator = TableOperators.And)
        {
            string query_filter = String.Empty;

            foreach (PropertyInfo property in DeclaredProperties)
            {
                string value = getProperty(property);
                if (value != null && value != String.Empty)
                {
                    if (query_filter == String.Empty)
                        query_filter = TableQuery.GenerateFilterCondition(property.Name, query_comparison, value);
                    else
                        query_filter = TableQuery.CombineFilters(query_filter, table_operator, TableQuery.GenerateFilterCondition(property.Name, query_comparison, value));
                }
            }
            if (!Utils.stringIsEmpty(PartitionKey))
            {
                if (query_filter == String.Empty)
                    query_filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, PartitionKey);
                else
                    query_filter = TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, PartitionKey), TableOperators.And, query_filter);
            }
            return query_filter;
        }

        protected List<string> _getQuerySelection()
        {
            List<string> selected_properties = new List<string>();

            foreach (PropertyInfo property in DeclaredProperties)
                if (getProperty(property) != null)
                    selected_properties.Add(property.Name);
            return selected_properties;
        }

        protected static int _sortPartitionComparer<T>(T a, T b) where T : TableObject, new()
        {
            return String.Compare(a.PartitionKey, b.PartitionKey);
        }

        /// <summary>
        /// Queries the table using a custom query filter and selection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query_filter">The query filter created e.g. TableQuery.GenerateFilterCondition.</param>
        /// <param name="query_selection">The query selection to return as a list of property names or null for all.</param>
        /// <returns>List of elements</returns>
        public static List<T> query<T>(string query_filter = "", List<String> query_selection = null) where T : TableObject, new()
        {
            TableQuery query = new TableQuery();
            query.FilterString = query_filter;
            if (query_selection != null)
                query.SelectColumns = query_selection;
            CloudTable table = _getTable(typeof(T).Name);
            TableContinuationToken token = null;
            List<T> result = new List<T>();
            do
            {
                TableQuerySegment<DynamicTableEntity> results = table.ExecuteQuerySegmented(query, token);
                foreach (DynamicTableEntity entity in results)
                {
                    T item = new T();
                    item.copyFromTable(entity);
                    result.Add(item);
                }
                token = results.ContinuationToken;
            } while (token != null);
            return result;
        }

        /// <summary>
        /// Queries the table by using the object property values as filters. Non-empty values are matched using the given comparison and table operators, empty values are returned and null values are ignored.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query_comparison">The query comparison to use with properties e.g. QueryComparisons.Equal.</param>
        /// <param name="table_operator">The table operator e.g. TableOperators.And.</param>
        /// <returns>List of elements</returns>
        public List<T> queryByValues<T>(string query_comparison = QueryComparisons.Equal, string table_operator = TableOperators.And) where T : TableObject, new()
        {
            string query_filter = _getQueryFilter();
            List<string> query_selection = _getQuerySelection();
            return query<T>(query_filter, query_selection);
        }

        /// <summary>
        /// Iterates the table values using a custom query filter and selection and calls an update callback for each element.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="update_method">The update method that returns true if the element was changed and should be updated.</param>
        /// <param name="query_filter">The query filter created e.g. TableQuery.GenerateFilterCondition.</param>
        /// <param name="query_selection">The query selection to return as a list of property names or null for all.</param>
        /// <returns>Number of updated elements</returns>
        public static int iterate<T>(Func<T, bool> update_method, string query_filter = "", List<String> query_selection = null) where T : TableObject, new()
        {
            if (update_method == null)
                return 0;

            TableBatchOperation batch_update = new TableBatchOperation();
            TableQuery query = new TableQuery();
            query.FilterString = query_filter;
            if (query_selection != null)
                query.SelectColumns = query_selection;
            CloudTable table = _getTable(typeof(T).Name);
            TableContinuationToken token = null;
            int update_count = 0;
            T element = new T();

            do
            {
                TableQuerySegment<DynamicTableEntity> results = table.ExecuteQuerySegmented(query, token);
                foreach (DynamicTableEntity entity in results)
                {
                    element.copyFromTable(entity);
                    if (update_method(element))
                    {
                        update_count++;
                        batch_update.InsertOrMerge(element);
                        if (batch_update.Count >= 100)
                        {
                            table.ExecuteBatch(batch_update);
                            batch_update.Clear();
                        }
                        element = new T();
                    }
                }
                token = results.ContinuationToken;
            } while (token != null);
            if (batch_update.Count > 0)
                table.ExecuteBatch(batch_update);
            return update_count;
        }

        /// <summary>
        /// Iterates the table by using the object property values as filters and calls an update callback for each element. Non-empty values are matched using the given comparison and table operators, empty values are returned and null values are ignored.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="update_method">The update method that returns true if the element was changed and should be updated.</param>
        /// <param name="query_comparison">The query comparison.</param>
        /// <param name="table_operator">The table operator.</param>
        /// <returns>Number of updated elements</returns>
        public int iterateByValues<T>(Func<T, bool> update_method, string query_comparison = QueryComparisons.Equal, string table_operator = TableOperators.And) where T : TableObject, new()
        {
            string query_filter = _getQueryFilter();
            List<string> query_selection = _getQuerySelection();
            return iterate<T>(update_method, query_filter, query_selection);
        }

        /// <summary>
        /// Iterates the given elements calls an update callback for each element.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="elements">The elements.</param>
        /// <param name="update_method">The update method that returns true if the element was changed and should be updated.</param>
        /// <returns>Number of updated elements</returns>
        public static int iterateByElements<T>(List<T> elements, Func<T, bool> update_method) where T : TableObject, new()
        {
            int updates = 0;
            if (elements == null || elements.Count == 0 || update_method == null)
                return updates;

            CloudTable table = _getTable(typeof(T).Name);
            TableQuery query = new TableQuery();
            TableBatchOperation batch_query = new TableBatchOperation();
            TableBatchOperation batch_update = new TableBatchOperation();
            List<T> partition_elements = new List<T>();
            elements.Sort(_sortPartitionComparer<T>);
            
            int i = 0;

            StringBuilder query_builder = new StringBuilder();
            while (i < elements.Count)
            {
                query_builder.Clear();
                string partition_key = elements[i].PartitionKey;
                while (i < elements.Count && partition_elements.Count <= 100 && elements[i].PartitionKey == partition_key)
                {
                    string element_filter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, elements[i].RowKey);
                    if (query_builder.Length > 0)
                        query_builder.Append(" ").Append(TableOperators.Or).Append(" ");
                    query_builder.Append(element_filter);
                    i++;
                }
                string query_filter = TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partition_key), TableOperators.And, query_builder.ToString());
                List<T> query_results = query<T>(query_filter.ToString(), null);
                batch_query.Clear();
                foreach (T result in query_results)
                {
                    if (update_method(result))
                    {
                        batch_update.InsertOrMerge(result);
                        updates++;
                    }
                }
                if (batch_update.Count > 0)
                    table.ExecuteBatch(batch_update);
                batch_update.Clear();
            }
            return updates;
        }

        /// <summary>
        /// What should happen on insert operation when encountering duplicates
        /// </summary>
        public enum AzureInsertMode { fail, replace, merge }
        /// <summary>
        /// Inserts the specified elements.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="elements">The elements.</param>
        /// <param name="duplicate_mode">If there are duplicates, fail, replace or merge element.</param>
        public static void insert<T>(List<T> elements, AzureInsertMode duplicate_mode = AzureInsertMode.fail) where T : TableObject, new()
        {
            TableBatchOperation batch_update = new TableBatchOperation();
            CloudTable table = _getTable(typeof(T).Name);
            elements.Sort(_sortPartitionComparer<T>);

            int i = 0;

            while (i < elements.Count)
            {
                string partition_key = elements[i].PartitionKey;
                while (i < elements.Count && batch_update.Count <= 100 && elements[i].PartitionKey == partition_key)
                {
                    if (duplicate_mode == AzureInsertMode.replace)
                        batch_update.InsertOrReplace(elements[i]);
                    else if (duplicate_mode == AzureInsertMode.merge)
                        batch_update.InsertOrMerge(elements[i]);
                    else
                        batch_update.Insert(elements[i]);
                    i++;
                }
                table.ExecuteBatchAsync(batch_update);
                batch_update.Clear();
            }
        }

        /// <summary>
        /// Reloads the element if older than specified period.
        /// </summary>
        /// <param name="period">The period in seconds.</param>
        /// <returns>True if loaded</returns>
        public virtual bool refresh(double period)
		{
			if (!isLoadedSince(DateTime.Now.AddSeconds(-period)))
				return load();
			else
				return false;
		}
        /// <summary>
        /// Loads the element from the table.
        /// </summary>
        /// <returns>True if loaded</returns>
        public virtual bool load()
        {
            error = "";
            try {
                TableOperation retrieve = TableOperation.Retrieve(this.PartitionKey, this.RowKey);
                TableResult result = _table.Execute(retrieve);
                if (result.Result != null) {
                    this.copyFromTable(result.Result as DynamicTableEntity);
					this.load_time = DateTime.Now;
                    return true;
                } else {
                    error = "TableObject.load: Azure returned HTTP status " + result.HttpStatusCode;
                    return false;
                }
            } catch (Exception e) {
                error = "TableObject.load: Exception '" + e.Message + "'";
                return false;
            }
        }

        /// <summary>
        /// Saves the element to the table.
        /// </summary>
        /// <returns>True if saved</returns>
        public virtual bool save()
        {
            error = "";
            try
            {
                TableOperation operation = TableOperation.InsertOrReplace(this);
                TableResult result = _table.Execute(operation);
                bool success = result.HttpStatusCode >= 200 && result.HttpStatusCode < 300;
                if (!success)
                    error = "TableObject.save: Azure returned HTTP status " + result.HttpStatusCode;
                return success;
            } catch (Exception e) {
                error = "TableObject.save: Exception '" + e.Message + "'";
                return false;
            }
        }

        /// <summary>
        /// Delete the element from the table.
        /// </summary>
        /// <returns>True if deleted</returns>
        public virtual bool delete()
        {
            if (this.ETag == null)
                this.ETag = "*";

            error = "";
            try
            {
                TableOperation operation = TableOperation.Delete(this);
                TableResult result = _table.Execute(operation);
                bool success = result.HttpStatusCode >= 200 && result.HttpStatusCode < 300;
                if (!success)
                    error = "TableObject.delete: Azure returned HTTP status " + result.HttpStatusCode;
                return success;
            } catch (Exception e) {
                error = "TableObject.delete: Exception '" + e.Message + "'";
                return false;
            }
        }
    }

    /// <summary>
    /// Base class for Azure queue objects.
    /// </summary>
    /// <seealso cref="RationalZone.v4.AzureObjects.IAzureObject" />
    public abstract class QueueObject: IAzureObject
    {
        private static DateTime _defalt_datetime = new DateTime(1900, 1, 1);

        protected static CloudStorageAccount _account = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["dotnet-azure-objects.connectionstring"]);
        protected static CloudQueueClient _client = _account.CreateCloudQueueClient();
        protected static Dictionary<string, CloudQueue> _queues = new Dictionary<string, CloudQueue>();

        protected CloudQueue _queue = null;
        protected PropertyInfo[] _properties = null;
        protected CloudQueueMessage _message = null;

        [IgnoreProperty]
        public PropertyInfo[] DeclaredProperties { get { return _properties; } }
        [IgnoreProperty]
        public abstract NameValueCollection DefaultValues { get; }

        public string error { get; set; }

        [IgnoreProperty]
        public DateTime load_time { get; set; }
        public string id { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueObject"/> class.
        /// </summary>
        public QueueObject()
        {
            _properties = AzureHelpers._getDeclaredProperties(this.GetType());
            _queue = getQueue(this.GetType().Name);
            error = "";
        }

        protected static CloudQueue getQueue(string queue_name)
        {
            queue_name = queue_name.ToLower();
            if (_queues.ContainsKey(queue_name))
                return _queues[queue_name];

            CloudQueue queue = _client.GetQueueReference(queue_name);
            queue.CreateIfNotExists();
            _queues[queue_name] = queue;
            return queue;
        }

        /// <summary>
        /// Gets the property by key name.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public string getProperty(string key)
        {
            return AzureHelpers._getProperty(this, key);
        }
        /// <summary>
        /// Gets the property by PropertyInfo.
        /// </summary>
        /// <param name="property">The property.</param>
        /// <returns></returns>
        public string getProperty(PropertyInfo property)
        {
            return AzureHelpers._getProperty(this, property);
        }
        /// <summary>
        /// Sets the property by key name.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public void setProperty(string key, string value)
        {
            AzureHelpers._setProperty(this, key, value);
        }
        /// <summary>
        /// Sets the property by PropertyInfo.
        /// </summary>
        /// <param name="property">The property.</param>
        /// <param name="value">The value.</param>
        public void setProperty(PropertyInfo property, string value)
        {
            AzureHelpers._setProperty(this, property, value);
        }
        /// <summary>
        /// Adds properties from a NameValueCollection.
        /// </summary>
        /// <param name="collection">The collection.</param>
        public void addCollection(NameValueCollection collection)
        {
            AzureHelpers._addCollection(this, collection);
        }
        /// <summary>
        /// Exports properties as json.
        /// </summary>
        /// <param name="keys">The keys.</param>
        /// <returns>Selected properties as JSON-string</returns>
        public string exportAsJson(string[] keys)
        {
            return AzureHelpers._exportAsJson(this, keys);
        }
        /// <summary>
        /// Exports properties as json.
        /// </summary>
        /// <returns>Properties as JSON-string</returns>
        public string exportAsJson()
        {
            return AzureHelpers._exportAsJson(this);
        }
        /// <summary>
        /// Exports properties as dictionary.
        /// </summary>
        /// <returns>Dictionary of property values</returns>
        public Dictionary<string, string> exportAsDictionary()
        {
            return AzureHelpers._exportAsDictionary(this);
        }
        /// <summary>
        /// Exports properties as HTML.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="name_tag">The name tag e.g. '<td>'.</param>
        /// <param name="value_tag">The value tag e.g. '<td>'.</param>
        /// <param name="value_delimeter">The value delimeter e.g. '='.</param>
        /// <param name="property_delimeter">The property delimeter e.g. '<br />'.</param>
        /// <returns>HTML-string</returns>
        public string exportAsHtml(IAzureObject obj, string name_tag = "", string value_tag = "", string value_delimeter = "=", string property_delimeter = "<br />")
        {
            return AzureHelpers._exportAsHtml(this, name_tag, value_tag, value_delimeter, property_delimeter);
        }
        /// <summary>
        /// Copies properties from a DynamicTableEntity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        public void copyFromTable(DynamicTableEntity entity)
        {
            AzureHelpers._copyFromTable(this, entity);
        }
        /// <summary>
        /// Copies properties from a HTTP request parameters.
        /// </summary>
        /// <param name="request">The request.</param>
        public void copyFromHttp(HttpRequest request)
        {
            AzureHelpers._copyFromHttp(this, request);
        }
        /// <summary>
        /// Copies properties from json-data.
        /// </summary>
        /// <param name="json_data">The json-data.</param>
        public void copyFromJson(string json_data)
        {
            AzureHelpers._copyFromJson(this, json_data);
        }
        /// <summary>
        /// Copies properties from ini-data.
        /// </summary>
        /// <param name="ini_data">The ini-data.</param>
        public void copyFromIni(string ini_data)
        {
            AzureHelpers._copyFromIni(this, ini_data);
        }
        /// <summary>
        /// Copies properties from a string-dictionary.
        /// </summary>
        /// <param name="data">The dictionary-data.</param>
        public void copyFromDictionary(Dictionary<string, string> data)
        {
            AzureHelpers._copyFromDictionary(this, data);
        }

        /// <summary>
        /// Peeks an element from the queue.
        /// </summary>
        /// <returns></returns>
        public virtual bool peek()
        {
            try
            {
                _message = _queue.PeekMessage();
                if (_message != null)
                {
                    copyFromIni(_message.AsString);
                    load_time = DateTime.Now;
                    return true;
                }
                else
                {
                    error = "QueueObject.peek: Azure returned no messages!";
                    return false;
                }
            }
            catch (Exception e)
            {
                error = "QueueObject.peek: Exception '" + e.Message + "'";
                return false;
            }
        }

        /// <summary>
        /// Pops an element from the queue.
        /// </summary>
        /// <returns></returns>
        public virtual bool pop()
        {
            try
            {
                _message = _queue.GetMessage();
                if (_message != null)
                {
                    copyFromIni(_message.AsString);
                    load_time = DateTime.Now;
                    return true;
                }
                else
                {
                    error = "QueueObject.pop: Azure returned no messages!";
                    return false;
                }
            }
            catch (Exception e)
            {
                error = "QueueObject.pop: Exception '" + e.Message + "'";
                return false;
            }
        }

        /// <summary>
        /// Pushes the specified dictionary values to the queue.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="values">The values.</param>
        /// <returns></returns>
        public static bool push<T>(Dictionary<string, string> values, int expiration_time = 30*24*60*60, int visibility_delay = 0) where T : QueueObject, new()
        {
            T obj = new T();
            obj.copyFromDictionary(values);
            return obj.push(expiration_time, visibility_delay);
        }
        /// <summary>
        /// Pushes the element to the queue.
        /// </summary>
        /// <param name="expiration_time">The expiration time.</param>
        /// <param name="visibility_delay">The visibility delay.</param>
        /// <returns></returns>
        public virtual bool push(int expiration_time = 30*24*60*60, int visibility_delay = 0)
        {
            try
            {
                string message_data = AzureHelpers._exportAsIni(this);
                if (_message == null)
                {
                    _message = new CloudQueueMessage(message_data);
                    _queue.AddMessage(_message, null, TimeSpan.FromSeconds(visibility_delay));
                    return true;
                }
                else
                {
                    _message.SetMessageContent(message_data);
                    _queue.UpdateMessage(_message, TimeSpan.FromSeconds(visibility_delay), MessageUpdateFields.Content | MessageUpdateFields.Visibility);
                    return true;
                }
            }
            catch (Exception e)
            {
                error = "QueueObject.push: Exception '" + e.Message + "'";
                return false;
            }
        }

        /// <summary>
        /// Removes the popped element from the queue.
        /// </summary>
        public virtual void remove()
        {
            try
            {
                if (_message != null)
                {
                    _queue.DeleteMessage(_message);
                    _message = null;
                }
            }
            catch (Exception e)
            {
                error = "QueueObject.remove: Exception '" + e.Message + "'";
            }
        }

    }

}

