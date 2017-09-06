package seu.an.hdfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class test {

	
	public static void main(String[] args) throws Throwable {
		// TODO Auto-generated method stub
		//readFromServer();
		putFileToServer();
		//creatDir();
		listDirOrFile();
	}

	//读取文件
	private static void readFromServer() throws Throwable {
		URI uri = new URI("hdfs://192.168.233.129:9000/");

		FileSystem fsFileSystem = FileSystem.get(uri, new Configuration(),"livean");
		
		FSDataInputStream fsDataInputStream = fsFileSystem.open(new Path(
				"hdfs://192.168.233.129 :9000/hello2"));
		
		IOUtils.copyBytes(fsDataInputStream, System.out, new Configuration(), false);
	}
	
	//上传文件
	private static void putFileToServer() throws Throwable {
		URI uri = new URI("hdfs://192.168.233.129:9000/");

		FileSystem fsFileSystem = FileSystem.get(uri, new Configuration(),"livean");
		
		//以内存为中心,先将硬盘文件读入Stream
		FileInputStream fileInputStream = new FileInputStream("file2.txt");
		//再将Stream上传
		FSDataOutputStream fsDataOutputStream = fsFileSystem.create(new Path(
				"hdfs://192.168.233.129:9000/dir/file2.txt"));
		
		IOUtils.copyBytes(fileInputStream, fsDataOutputStream, new Configuration(), true);
		
		System.out.println("upload success!");
	}
	
	//创建目录
	private static void creatDir() throws Throwable {
		URI uri = new URI("hdfs://192.168.233.129:9000/");

		FileSystem fsFileSystem = FileSystem.get(uri, new Configuration(),"livean");
		
		fsFileSystem.mkdirs(new Path("hdfs://192.168.233.129:9000/he"));
		System.out.println("creat dir success!");
	}
	
	//列出目录或文件
	private static void listDirOrFile() throws Throwable {
		URI uri = new URI("hdfs://192.168.233.129:9000/");

		FileSystem fsFileSystem = FileSystem.get(uri, new Configuration(),"livean");
		
		FileStatus[] fileStatuses = fsFileSystem.listStatus(new Path(
				"hdfs://192.168.233.129:9000/"));
		for(FileStatus fileStatus : fileStatuses)
		{
			String type = fileStatus.isDirectory()?"目录":"文件";
			String name = fileStatus.getPath().getName();
			System.out.println(type+"\t"+name);
		}
	}
}