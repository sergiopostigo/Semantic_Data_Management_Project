import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.util.iterator.ExtendedIterator;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.*;
import java.lang.reflect.Array;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class KnowledgeGraph {

    //Knowledge Graph Model
    OntModel kg_model;

    // Constructor
    public KnowledgeGraph(String SOURCE) {

        // Ontology namespace
        String NS = SOURCE + "#";

        // Create the base model
        kg_model = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM);

        // Create the TBOX
        CreateTBOX(NS);

        // Create the ABOX
        CreateABOX(NS);




    }

    public void CreateTBOX(String NS){

        // Creating classes and subclasses
        OntClass good = kg_model.createClass(NS + "Good");
        OntClass customs_description = kg_model.createClass(NS + "Customs_Description");
        OntClass via = kg_model.createClass(NS + "Via");
        OntClass country = kg_model.createClass(NS + "Country");
        OntClass company = kg_model.createClass(NS + "Company");
        OntClass trader = kg_model.createClass(NS + "Trader");
        OntClass shipper = kg_model.createClass(NS + "Shipper");
        company.addSubClass(shipper);
        company.addSubClass(trader);

        // Creating Object Type Properties
        ObjectProperty belongs_to = kg_model.createObjectProperty(NS + "belongs_to");
        belongs_to.addDomain(good);
        belongs_to.addRange(customs_description);

        ObjectProperty sent_by = kg_model.createObjectProperty(NS + "sent_by");
        sent_by.addDomain(good);
        sent_by.addRange(via);

        ObjectProperty comes_from = kg_model.createObjectProperty(NS + "comes_from");
        comes_from.addDomain(good);
        comes_from.addRange(country);

        ObjectProperty goes_to = kg_model.createObjectProperty(NS + "goes_to");
        goes_to.addDomain(good);
        goes_to.addRange(country);

        ObjectProperty transports = kg_model.createObjectProperty(NS + "transports");
        transports.addDomain(shipper);
        transports.addRange(good);

        ObjectProperty imports = kg_model.createObjectProperty(NS + "imports");
        imports.addDomain(trader);
        imports.addRange(good);

        ObjectProperty exports = kg_model.createObjectProperty(NS + "exports");
        exports.addDomain(trader);
        exports.addRange(good);

        // Creating Data Type Properties
        DatatypeProperty date = kg_model.createDatatypeProperty(NS +"date");
        date.addDomain(good);

        DatatypeProperty cost = kg_model.createDatatypeProperty(NS +"cost");
        cost.addDomain(good);

        DatatypeProperty weight = kg_model.createDatatypeProperty(NS +"weight");
        weight.addDomain(good);

        DatatypeProperty commercial_description = kg_model.createDatatypeProperty(NS +"commercial_description");
        commercial_description.addDomain(good);

        DatatypeProperty country_name = kg_model.createDatatypeProperty(NS +"name");
        country_name.addDomain(country);

        DatatypeProperty company_name = kg_model.createDatatypeProperty(NS +"name");
        company_name.addDomain(company);

        DatatypeProperty text = kg_model.createDatatypeProperty(NS +"text");
        text.addDomain(customs_description);


    }

    public void CreateABOX(String NS){

        // Path to data
        String data_path = "../data/peru_data.csv";
        // Read the data
        List<String[]>  data = readCSV(data_path).subList(0,20000);
        // ---------------------------------------
        // Trader
        // ---------------------------------------
        ArrayList<String> traders = new ArrayList<String>();
        // Get all traders
        data.forEach(record -> {
            traders.add(record[7]);
        });
        // Remove duplicated traders
        Set<String> tradersWithoutDuplicates = new LinkedHashSet<String>(traders);
        traders.clear();
        traders.addAll(tradersWithoutDuplicates);
        // Create individuals
        traders.forEach(trader -> {
            // Get the class
            OntClass traderClass = kg_model.getOntClass( NS + "Trader" );
            // Generate a random id
            Random rnd = new Random();
            long id = Instant.now().toEpochMilli()+rnd.nextInt(9999999);
            // Create the instance
            Individual traderIndividual = kg_model.createIndividual(NS + "Trader/" + id, traderClass);
            // Create the corresponding literals
            Literal nameLiteral = kg_model.createLiteral(trader);
            // Link the instance with the literals
            traderIndividual.addProperty(kg_model.getProperty(NS +"name"), nameLiteral);

        });
        // ---------------------------------------

        // ---------------------------------------
        // Shipper
        // ---------------------------------------
        ArrayList<String> shippers = new ArrayList<String>();
        // Get all traders
        data.forEach(record -> {
            shippers.add(record[6]);
        });
        // Remove duplicated shippers
        Set<String> shippersWithoutDuplicates = new LinkedHashSet<String>(shippers);
        shippers.clear();
        shippers.addAll(shippersWithoutDuplicates);
        // Remove nan value (if any, )
        shippers.remove("nan");
        // Create individuals
        shippers.forEach(shipper -> {
            // Get the class
            OntClass shipperClass = kg_model.getOntClass( NS + "Shipper" );
            // Generate a random id
            Random rnd = new Random();
            long id = Instant.now().toEpochMilli()+rnd.nextInt(9999999);
            // Create the instance
            Individual shipperIndividual = kg_model.createIndividual(NS + "Shipper/" + id, shipperClass);
            // Create the corresponding literals
            Literal nameLiteral = kg_model.createLiteral(shipper);
            // Link the instance with the literals
            shipperIndividual.addProperty(kg_model.getProperty(NS +"name"), nameLiteral);

        });
        // ---------------------------------------

        // ---------------------------------------
        // Country
        // ---------------------------------------
        ArrayList<String> countries = new ArrayList<String>();
        // Get all countries
        data.forEach(record -> {
            countries.add(record[3]);
            countries.add(record[4]);

        });
        // Remove duplicated countries
        Set<String> countriesWithoutDuplicates = new LinkedHashSet<String>(countries);
        countries.clear();
        countries.addAll(countriesWithoutDuplicates);
        // Remove None value (if any)
        countries.remove("None");
        // Create individuals
        countries.forEach(country -> {
            // Get the class
            OntClass countryClass = kg_model.getOntClass( NS + "Country" );
            // Generate a random id
            Random rnd = new Random();
            long id = Instant.now().toEpochMilli()+rnd.nextInt(9999999);
            // Create the instance
            Individual countryIndividual = kg_model.createIndividual(NS + "Country/" + id, countryClass);
            // Create the corresponding literals
            Literal nameLiteral = kg_model.createLiteral(country);
            // Link the instance with the literals
            countryIndividual.addProperty(kg_model.getProperty(NS +"name"), nameLiteral);
        });
        // ---------------------------------------

        // ---------------------------------------
        // Via
        // ---------------------------------------
        ArrayList<String> vias = new ArrayList<String>();
        // Get all vias
        data.forEach(record -> {
            vias.add(record[5]);
        });
        // Remove duplicated vias
        Set<String> viasWithoutDuplicates = new LinkedHashSet<String>(vias);
        vias.clear();
        vias.addAll(viasWithoutDuplicates);
        // Remove None value (if any)
        vias.remove("None");
        // Create individuals
        vias.forEach(via -> {
            // Get the class
            OntClass viaClass = kg_model.getOntClass( NS + "Via" );
            // Create the instance
            Individual viaIndividual = kg_model.createIndividual(NS + "Via/" + via, viaClass);
        });
        // ---------------------------------------

        // ---------------------------------------
        // Customs description
        // ---------------------------------------
        ArrayList<String> customs = new ArrayList<String>();
        // Get all vias
        data.forEach(record -> {
            customs.add(record[1]);
        });
        // Remove duplicated vias
        Set<String> customsWithoutDuplicates = new LinkedHashSet<String>(customs);
        customs.clear();
        customs.addAll(customsWithoutDuplicates);
        // Create individuals
        customs.forEach(custom -> {
            // Get the class
            OntClass customClass = kg_model.getOntClass( NS + "Customs_Description" );
            // Generate a random id
            Random rnd = new Random();
            long id = Instant.now().toEpochMilli()+rnd.nextInt(9999999);
            // Create the instance
            Individual customIndividual = kg_model.createIndividual(NS + "Customs_Description/" + id, customClass);
            // Create the corresponding literals
            Literal textLiteral = kg_model.createLiteral(custom);
            // Link the instance with the literals
            customIndividual.addProperty(kg_model.getProperty(NS +"text"), textLiteral);
        });
        // ---------------------------------------

        // ---------------------------------------
        // Good
        // ---------------------------------------
        ArrayList<String> goods = new ArrayList<String>();
        data.forEach(record -> {
            // Get the class
            OntClass goodClass = kg_model.getOntClass( NS + "Good" );
            // Generate a random id
            Random rnd = new Random();
            long id = Instant.now().toEpochMilli()+rnd.nextInt(9999999);
            // Create the instance
            Individual goodIndividual = kg_model.createIndividual(NS + "Good/" + id, goodClass);
            // Create the corresponding literals
            Literal commercialDescriptionLiteral = kg_model.createLiteral(record[9]);
            Literal weightLiteral = kg_model.createLiteral(record[10]);
            Literal costLiteral = kg_model.createLiteral(record[8]);
            // Add the data type properties
            goodIndividual.addProperty(kg_model.getProperty(NS +"commercial_description"), commercialDescriptionLiteral);
            goodIndividual.addProperty(kg_model.getProperty(NS +"weight"), weightLiteral);
            goodIndividual.addProperty(kg_model.getProperty(NS +"cost"), costLiteral);
            // Add the object type properties
            // --transports--
            // Get the individual to be associated to
            Individual shipper = getInstanceOfClassWithALiteral("Shipper", "name", record[6], NS);
            // If the individual does not exist, don´t add any property. If it exists, add the property
            if (shipper != null){
                shipper.addProperty(kg_model.getProperty(NS + "transports"), goodIndividual);
            }
            // ----
            // --imports--
            // Get the individual to be associated to
            Individual trader = getInstanceOfClassWithALiteral("Trader", "name", record[7], NS);
            // If the individual does not exist, don´t add any property. If it exists, add the property
            if (trader != null){
                trader.addProperty(kg_model.getProperty(NS + "imports"), goodIndividual);
            }
            // ----
            // --belongs_to--
            // Get the individual to be associated to
            Individual customs_description = getInstanceOfClassWithALiteral("Customs_Description", "text", record[1], NS);
            // If the individual does not exist, don´t add any property. If it exists, add the property
            if (customs_description != null){
                goodIndividual.addProperty(kg_model.getProperty(NS + "belongs_to"), customs_description);
            }
            // ----
            // --sent_by--
            // Get the individual to be associated to
            try {
                Individual via = kg_model.getIndividual(NS+"Via/"+record[5]);
                goodIndividual.addProperty(kg_model.getProperty(NS + "sent_by"),via);
            }
            catch(Exception e) {
                // In case the individual is not found, don´t instance the property
            }
            // ----
            // --comes_from--
            // Get the individual to be associated to
            Individual country_origin = getInstanceOfClassWithALiteral("Country", "name", record[3], NS);
            // If the individual does not exist, don´t add any property. If it exists, add the property
            if (country_origin != null){
                goodIndividual.addProperty(kg_model.getProperty(NS + "comes_from"), country_origin);
            }
            // ----
            // --goes_to--
            // Get the individual to be associated to
            Individual country_destiny = getInstanceOfClassWithALiteral("Country", "name", record[4], NS);
            // If the individual does not exist, don´t add any property. If it exists, add the property
            if (country_destiny != null){
                goodIndividual.addProperty(kg_model.getProperty(NS + "goes_to"), country_destiny);
            }

            // Check progress
            System.out.println( "Progress: " +Float.parseFloat(record[0])/data.size()*100+"%");
        });
        // ---------------------------------------
    }

    public ArrayList<String> OpenAvro(String path) {

        ArrayList<String> data = new ArrayList<String>();
        try {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(path), datumReader);
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                data.add(record.toString());
            }
        } catch (IOException e) {
            System.out.println("File not found");;
        }

        return data;
    }

    private List<String[]> readCSV(String path){

        List<String[]> list = null;
        try {
            FileReader filereader = new FileReader(path);
            CSVParser parser = new CSVParserBuilder().withSeparator(',').build();
            CSVReader csvReader = new CSVReaderBuilder(filereader)
                    .withCSVParser(parser)
                    .withSkipLines(1)
                    .build();

            // Read all data at once
            list = csvReader.readAll();

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public void showClasses() {

        // Create an iterator object containing all classes in the TBOX
        ExtendedIterator classes = kg_model.listClasses();

        // Iterate through all the classes
        while (classes.hasNext()) {
            OntClass thisClass = (OntClass) classes.next();
            // Print the found class
            System.out.println("Found class: " + thisClass.toString());

        }

    }
    public void showClassesAndInstances() {

        // Create an iterator object containing all classes in the TBOX
        ExtendedIterator classes = kg_model.listClasses();

        // Iterate through all the classes
        while (classes.hasNext()) {
            OntClass thisClass = (OntClass) classes.next();
            // Print the found class
            System.out.println("Found class: " + thisClass.toString());

            // Create an iterator object containing all instances (also called individuals) of the current class
            ExtendedIterator instances = thisClass.listInstances();

            // Iterate through all the instances
            while (instances.hasNext()) {
                Individual thisInstance = (Individual) instances.next();
                // Print the found instance
                System.out.println("  Found instance: " + thisInstance.toString());

            }
        }

    }
    public void showClassesInstancesAndProperties() {

        // Create an iterator object containing all classes in the TBOX
        ExtendedIterator classes = kg_model.listClasses();

        // Iterate through all the classes
        while (classes.hasNext()) {
            OntClass thisClass = (OntClass) classes.next();
            // Print the found class
            System.out.println("Found class: " + thisClass.toString());

            // Create an iterator object containing all instances (also called individuals) of the current class
            ExtendedIterator instances = thisClass.listInstances();

            // Iterate through all the instances
            while (instances.hasNext()) {
                Individual thisInstance = (Individual) instances.next();
                // Print the found instance
                System.out.println("  Found instance: " + thisInstance.toString());

                // Create an iterator object containing all instances (also called individuals) of the current property
                ExtendedIterator properties = thisInstance.listProperties();

                // Iterate through all the properties
                while (properties.hasNext()) {
                    Statement thisProperty = (Statement) properties.next();
                    // Print the found property
                    System.out.println("    Found property: " + thisProperty.toString());

                }

            }
        }

    }
    public void showInstancesAndPropertiesOfClass(String class_) {

        OntClass thisClass = kg_model.getOntClass(class_);

        // Print the found class
        System.out.println("Found class: " + thisClass.toString());

        // Create an iterator object containing all instances (also called individuals) of the current class
        ExtendedIterator instances = thisClass.listInstances();

        // Iterate through all the instances
        while (instances.hasNext()) {
            Individual thisInstance = (Individual) instances.next();
            // Print the found instance
            System.out.println("  Found instance: " + thisInstance.toString());

            // Create an iterator object containing all instances (also called individuals) of the current property
            ExtendedIterator properties = thisInstance.listProperties();

            // Iterate through all the properties
            while (properties.hasNext()) {
                Statement thisProperty = (Statement) properties.next();
                // Print the found property
                System.out.println("    Found property: " + thisProperty.toString());

            }

        }

    }
    public Individual getInstanceOfClassWithALiteral(String class_, String property, String literal, String NS) {

        // Get the class
        OntClass thisClass = kg_model.getOntClass(NS+class_);

        // Create an iterator object containing all instances (also called individuals) of the current class
        ExtendedIterator instances = thisClass.listInstances();
        // Initialize the instance to be returned
        Individual chosenInstance = null;
        // Iterate through all the instances of the class
        while (instances.hasNext()) {
            Individual thisInstance = (Individual) instances.next();
            // Check if the instance property has as range the literal value indicated in the argument of the function
            if (thisInstance.hasProperty(kg_model.getProperty(NS + property), literal)){
                // If there is a property associated to the instance with range the literal value, return the instance
                chosenInstance = thisInstance;
                break;
            }
        }
        return chosenInstance;

    }
    public void exportModel(String output_path, String format)
    {
        OutputStream out = null;
        try {
            out = new FileOutputStream(output_path);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        kg_model.write(out,format) ;
    }
}


