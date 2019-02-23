dateFormatProperties.accdb is a dump of the values in the 168 .properties files that can be 
used by RegionalSettings.java to support date/time formats of various locales.

It is not used by the UCanAccess code itself. It is just a reference for developers to "see"
what is in the 168 .properties without having to open each one.

The code I used to generate the .accdb file was:

~~~java
Statement st = conn.createStatement();
st.executeUpdate("CREATE TABLE props (id COUNTER PRIMARY KEY, fileName TEXT(255), propName TEXT(255), propValue TEXT(255))");

PreparedStatement ps = conn.prepareStatement("INSERT INTO props (fileName, propName, propValue) VALUES (?,?,?)");

String propsFolderName = "C:\\Users\\Gord\\git\\ucanaccess\\src\\main\\resources\\net\\ucanaccess\\util\\format";
File propsFolder = new File(propsFolderName);
int i = 0;
for (String fileName : propsFolder.list()) {
    InputStream is = new FileInputStream(propsFolderName + "\\" + fileName);
    Properties props = new Properties();
    props.load(is);
    for (Object propName : props.keySet()) {
        ps.setString(1, fileName);
        ps.setString(2, propName.toString());
        ps.setString(3, props.get(propName).toString());
        ps.executeUpdate();
    }
    i++;
}
~~~
