import java.util.*;
import java.io.*;
import java.util.Collection;
import java.util.TreeMap;
import java.util.Iterator;

class FileDescription
{
	private String name;
	public long length;

FileDescription(String name, long length)
{
	this.name=name;
	this.length=length;
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

public static void main(String []args)
{
List<FileDescription> results = new ArrayList<FileDescription>();

File[] files = new File(".").listFiles();
//If this pathname does not denote a directory, then listFiles() returns null. 

for (File file : files) {
    if (file.isFile()) {
	results.add( new FileDescription(file.getName(), file.length()));
    }
}
	
Collections.sort(results, new Comparator<FileDescription>() {
    @Override
    public int compare(FileDescription z1, FileDescription z2) {
        if (z1.length > z2.length)
            return 1;
        if (z1.length < z2.length)
            return -1;
        return 0;
    }
});

for (FileDescription temp : results) {
		System.out.println(temp.getName());	
}

}
}


