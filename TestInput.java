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

File[] files = new File("/home/hduser/input_file").listFiles();
//If this pathname does not denote a directory, then listFiles() returns null. 
OutputStream out = null;

for (File file : files) {
    if (file.isFile()) {
	results.add( new FileDescription(file.getName(), file.length()));
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

writer.write(str);
//	out = new FileOutputStream("output.txt");
//		out.write(str);
	
for (FileDescription temp : results) {
		//System.out.println(temp.getName());	
	if(temp.tag==0)
		{
			writer.write( "hadoop fs -put " + inputDir + "/"+ temp.getName() + " " + outputDir + "\n");
			temp.tag=1;
		}
	
}

writer.close();
	}catch(IOException e){
      System.out.print("Exception");
   }
}
}


