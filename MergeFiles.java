import java.util.*;
import java.io.*;
import java.util.Collection;
import java.util.TreeMap;
import java.util.Iterator;
import java.lang.*;

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
	public void putVal(long length)
	{
		this.length = length;
	}
	public String toString()
	{
		return ("Name: " + name + "Len: " + length);
	}

}

public class MergeFiles
{

	static String inputDir = null;
	static String outputDir = null;
	static List<FileDescription> results = new ArrayList<FileDescription>();
	static List<FileDescription> finalResults = new ArrayList<FileDescription>();
	static HashMap<String, Integer> presentFiles = new HashMap<String, Integer>();
	

	public static void main(String []args)
	{

		int fileCount;
		int dirSpace;
		int totalSize=0;
		int dirCount=1;
		

		inputDir = args[0];		// input directory name
		outputDir = args[1];	// output directory
		dirSpace = Integer.parseInt(args[2]); 	// Space in directory
		/*dirSpace /= 2;
		dirSpace -= 200;*/
		File listFile = new File(inputDir, "existing_files.txt");

		File[] files = new File(args[0]).listFiles();
		//If this pathname does not denote a directory, then listFiles() returns null. 
		OutputStream out = null;
		try {

			if( !listFile.exists() )
				{
					listFile.createNewFile();
				}else
				{
					System.exit(0);
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
				totalSize += file.length();
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
		}catch(IOException e)
		{
			System.out.print("Exception");
		}

		fileCount = totalSize/dirSpace; 	// This is to obtain the number of processbatch_ files created.
		fileCount++;

		//System.out.println("totalSize = " + totalSize);

		int count=0;
		long writeSize=0;

		try{

		File writeFiles = new File(inputDir, "writeFile1");
		writeFiles.createNewFile();
		FileWriter writerFiles = new FileWriter(writeFiles.getAbsoluteFile(), true);
		BufferedWriter bw = new BufferedWriter(writerFiles);
		FileDescription fileDesc = new FileDescription("writeFile1", 0);

		FileWriter fw2 = new FileWriter(listFile.getAbsoluteFile(), true);              // Writer to write to files maintaining list of transferred files.
        BufferedWriter writer2 = new BufferedWriter(fw2);

		for (FileDescription temp : results){

			if(writeSize + temp.getVal() >= dirSpace)
			{
			//System.out.println(temp.getName());
				
			//System.out.println("file size exceeded" + count+2);
				bw.close();
				finalResults.add(fileDesc);
				writeSize=0;

				writeFiles = new File(inputDir, "writeFile"+String.valueOf(count+2));
				writerFiles = new FileWriter(writeFiles.getAbsoluteFile(), true);
				bw = new BufferedWriter(writerFiles);
				fileDesc = new FileDescription("writeFile"+String.valueOf(count+2), 0);
				count++;
			}
			//System.out.println(temp.getName());

			File readFile = new File(inputDir, temp.getName());
			FileReader readerFiles = new FileReader(readFile.getAbsoluteFile());
			BufferedReader br = new BufferedReader(readerFiles);
			//Stringbuilder fullFile = new Stringbuilder();
			String line;

			while((line = br.readLine()) != null){

				//fullFile.append(line);
				bw.write("\n" + line);
			}
			br.close();

			//System.out.println("WS = " + writeSize);
			//bw.write(fullFile.toString());
			writeSize += temp.getVal();
			fileDesc.putVal(writeSize);
			writer2.write(temp.getName() + "\n");
		}
		writer2.close();
		finalResults.add(fileDesc);			
		bw.close();

		}catch(IOException e)
		{
			System.out.print("Exception");
		}

		try
		{
			File newFile = new File(inputDir, "processbatch_" + String.valueOf(fileCount) + ".sh");
			fileCount--;

			newFile.createNewFile();
			String str = "#!/bin/bash" + "\n\n";

			FileWriter fw = new FileWriter(newFile.getAbsoluteFile());				// Actual writing to the shell script file.
			BufferedWriter writer = new BufferedWriter(fw);		

			FileWriter fw2 = new FileWriter(listFile.getAbsoluteFile(), true);		// Writer to write to files maintaining list of transferred files.
			BufferedWriter writer2 = new BufferedWriter(fw2);
			long dataSize=0;

			writer.write(str);
			//	out = new FileOutputStream("output.txt");
			//		out.write(str);
				
			for (FileDescription temp : finalResults) 
			{
				//System.out.println(temp.getName() + temp.getVal());	
				dataSize += temp.getVal();

				//System.out.println("dirSpace = " + dirSpace + "Datasize = " + dataSize);

				if(dirSpace <= dataSize)			// If the space in HDFS dir is less than or equal to the sum of file sizes here, then open a new file.
				{
					//System.out.println("Making new file.");
					writer.write("/home/hduser/hadoop/bin/hadoop fs -chmod -R 777 " + outputDir + "/map-reduce_" + String.valueOf(fileCount+1) + "/");
					writer.close();
					dirCount++;

					newFile = new File(inputDir, "processbatch_" + String.valueOf(fileCount) + ".sh");
					newFile.createNewFile();
					fileCount--;
					
					String strForFiles = "if ~/process/checkFolder.sh '" + outputDir + "/map-reduce_" + String.valueOf(fileCount+1)  + "'; then\necho found\n\texit 0\nfi\n\n";  // Edit this line to put exact words.
					fw = new FileWriter(newFile.getAbsoluteFile());
					writer = new BufferedWriter(fw);		
					
					writer.write(str);
					writer.write(strForFiles);
					dataSize = temp.getVal();
				}

				//System.out.println(" Beginning to write ");
				writer.write( "/home/hduser/hadoop/bin/hadoop fs -put " + inputDir + "/"  + temp.getName() + " " + outputDir + "/map-reduce_" + String.valueOf(fileCount+1) + "/\n");
				writer.write( "rm " + inputDir + "/" + temp.getName() + "\n");
				writer2.write(temp.getName() + "\n");
			}

			writer.write("/home/hduser/hadoop/bin/hadoop fs -chmod -R 777 " + outputDir + "/map-reduce_" + String.valueOf(fileCount+1) + "/");
			writer2.close();
			writer.close();

		}catch(IOException e)
		{
	    	System.out.print("Exception");
	 	}	
	} // main
} // class


