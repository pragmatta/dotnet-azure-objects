# Rational Azure Objects

## Introduction
Rational Azure Objects is a wrapper for Azure Table and Queue object. Purpose is to provide a simple base class that allows you to quickly derive child classes by just introducing the data properties. No need to manage tables or queues, just start creating object instances and storing them to Azure. Also Rational Azure Objects provides a simple way manipulate data sets like
* Tables
  * Insert a collection of elements
  * Query elements based on property values
  * Iterate and update table elements
  * Copy properties from various collections
  * Export data in various formats
* Queues
  * Simple methods to peek, pop, push and remove elements from queue
  * Copy properties from various collections
  * Export data in various formats

## Getting Started

### Configure Storage Account
In your ConfigurationManager AppSettings add the Azure storage account connection string as key "dotnet-azure-objects.connectionstring". For example in your website web.config appSettings-section add line

```
<add key="dotnet-azure-objects.connectionstring" value="DefaultEndpointsProtocol=https;AccountName=myaccountname;AccountKey=...;EndpointSuffix=core.windows.net" />
```

### Create A Class
First thing you need to do is to derive a class from one of the base classes: 
```
public class Person : TableObject
{
	protected NameValueCollection _defaultValues = new NameValueCollection();
	public override NameValueCollection DefaultValues { get { return _defaultValues; } }

	public Person(string id) : base(id) { }

	public string name { get; set; }
	public string car { get; set; }
}
```

### Create An Instance
To create a table just create an instance of the class and save it! 
```
Person person = new Person("Foo");
person.name = "Bar";
person.name = "Toyota";
person.save();
```

### Manipulate Data
To fetch data from the table based on a property value, create an element and set the values you want to search and empty the properties you need.
```
Person person = new Person();
person.name = "";
person.car = "Honda";
List<Person> honda_owners = person.queryByValues();
```

To iterate through all data and update some values fetch data from the table based on a property value, create an element and set the values you want to search and empty the properties you need.
```
Func<Person, bool> person_iterator = person =>
{
	if (person.car == "Honda Jazz")
	{
		person.car == "Honda Fit"
		return true;
	}
	return false;
};

Person person = new Person();
person.car = "";
person.iterateByValues(person_iterator);
```

## Documentation
See [**https://github.com/pragmatta/dotnet-azure-objects/blob/master/documentation/html/annotated.html**](http://htmlpreview.github.io/?https://github.com/pragmatta/dotnet-azure-objects/blob/master/documentation/html/annotated.html) for more
