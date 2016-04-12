import java.util.*;
import java.io.*;
import java.util.Collection;
import java.util.TreeMap;
import java.util.Iterator;

class FileDescription
{
	private String name;
	public long length;
	public int tag;

FileDescription(String name, long length)
{
	this.name=name;
	this.length=length;
	this.tag=0;
}

public String getName()
{
	return name;
}

public long getVal()
{
	return length;
}


public String toString()
{
	 return ("Name: " + name + "Len: " + length);

}

}

public class TestInput
{

static String inputDir = null;
static String outputDir = null;

public static void main(String []args)
{


inputDir = args[0];
outputDir = args[1];

List<FileDescription> results = new ArrayList<FileDescription>();
HashMap<String, Integer> presentFiles = new HashMap<String, Integer>();


File[] files = new File(args[0]).listFiles();
//If this pathname does not denote a directory, then listFiles() returns null. 
OutputStream out = null;

try {

File listFile = new File(".", "existing_files.txt");

if( !listFile.exists() )
	{
		listFile.createNewFile();
	}

BufferedReader br = new BufferedReader(new FileReader(listFile));
    String line;
    while ((line = br.readLine()) != null) {
       // process the line.
	presentFiles.put(line, 1);
    }
br.close();

for (File file : files) {
    if (file.isFile() && !(presentFiles.containsKey(file.getName()))) {
		
	results.add( new FileDescription(file.getName(), file.length()));
	//System.out.println(" This file not present: " + file.getName());
    }
}
	
Collections.sort(results, new Comparator<FileDescription>() {
    @Override
    public int compare(FileDescription z1, FileDescription z2) {
        if (z1.length < z2.length)
            return 1;
        if (z1.length > z2.length)
            return -1;
        return 0;
    }
});


try
{
File newFile = new File(".", "output.sh");
newFile.createNewFile();
String str = "#!/bin/bash" + "\n\n";

FileWriter fw = new FileWriter(newFile.getAbsoluteFile());
BufferedWriter writer = new BufferedWriter(fw);


FileWriter fw2 = new FileWriter(listFile.getAbsoluteFile(), true);
BufferedWriter writer2 = new BufferedWriter(fw2);

writer.write(str);
//	out = new FileOutputStream("output.txt");
//		out.write(str);
	
for (FileDescription temp : results) {
		//System.out.println(temp.getName());	
		writer.write( "hadoop fs -put " + inputDir + "/"+ temp.getName() + " " + outputDir + "\n");
		writer2.write(temp.getName() + "\n");
	
}

writer2.close();
writer.close();

	}catch(IOException e){
      System.out.print("Exception");
   }

	}catch(IOException e){
      System.out.print("Exception");
   }

} // main
} // class


