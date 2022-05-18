import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

public class DBTable {
	private RandomAccessFile rows;	//the file that stores the rows in the table
	private long free;	//head of the free list space for rows
	private int numOtherFields;
	private int otherFieldLengths[];
	private BTree tree;
	
	private class Row {
		private int keyField;
		private char otherFields[][];
		
		public Row(int k, char fields[][]) {
			keyField = k;
			otherFields = fields;
		}
		
		public Row(long addr) throws IOException {
			rows.seek(addr);
			keyField = rows.readInt();
			
			otherFields = new char[numOtherFields][];
			for(int i = 0; i < numOtherFields; i++) {
				otherFields[i] = new char[otherFieldLengths[i]];
				for(int j = 0; j < otherFieldLengths[i]; j++) {
					otherFields[i][j] = rows.readChar();
				}
			}
		}
		
		private void writeRow(long addr) throws IOException {
			rows.seek(addr);
			rows.writeInt(keyField);
			
			for(int i = 0; i < otherFields.length; i++) {
				for(int j = 0; j < otherFields[i].length; j++) {
					rows.writeChar(otherFields[i][j]);
				}
			}
		}
	}
	
	public DBTable(String filename, int fl[], int bSize) throws IOException {
		rows = new RandomAccessFile(filename, "rw");
		free = 0;
		rows.writeLong(free);
		
		numOtherFields = fl.length;
		rows.writeInt(numOtherFields);
		otherFieldLengths = new int[numOtherFields];
		for(int i = 0; i < numOtherFields; i++) {
			otherFieldLengths[i] = fl[i];
			rows.writeInt(otherFieldLengths[i]);
		}
		tree = new BTree(filename + "BTree", bSize);
	}
	
	public DBTable(String filename) throws IOException {
		rows = new RandomAccessFile(filename, "rw");
		rows.seek(0);
		free = rows.readLong();
		
		numOtherFields = rows.readInt();
		otherFieldLengths = new int[numOtherFields];
		for(int i = 0; i < numOtherFields; i++) {
			otherFieldLengths[i] = rows.readInt();
		}
		filename += "BTree";
		tree = new BTree(filename);
	}
	
	public boolean insert(int key, char fields[][]) throws IOException {
		long r = getFree();
		removeFromFree();
		rows.seek(r);
		Row toInsert = new Row(key, fields);
		toInsert.writeRow(r);
		return tree.insert(key, r);
	}
	
	public boolean remove(int key) throws IOException {
		long addr = tree.remove(key);
		if(addr == 0) return false;
		addToFree(addr);
		return true;
	}
	
	public LinkedList<String> search(int key) throws IOException {
		LinkedList<String> toReturn = new LinkedList<>();
		long loc = tree.search(key);
		if(loc == 0) return toReturn;
		return addFields(loc, toReturn);
	}
	
	public LinkedList<LinkedList<String>> rangeSearch(int low, int high) throws IOException {
		LinkedList<Long> addrs = tree.rangeSearch(low, high);
		LinkedList<LinkedList<String>> toReturn = new LinkedList<>();
		LinkedList<String> toAdd;
		Row cur;
		for(long addr: addrs) {
			toAdd = new LinkedList<>();
			cur = new Row(addr);
			toAdd.add(cur.keyField + "");
			toReturn.add(addFields(addr, toAdd));
		}
		return toReturn;
	}

	private LinkedList<String> addFields(long addr, LinkedList<String> list) throws IOException {
		Row cur = new Row(addr);
		for(int i = 0; i < cur.otherFields.length; i++) {
			String temp = "";
			for(int j = 0; j < cur.otherFields[i].length; j++) {
				if(cur.otherFields[i][j] != '\0') temp += cur.otherFields[i][j];
			}
			if(temp != "") list.add(temp);
		}
		return list;
	}
	
	private long getFree() throws IOException {
		return free == 0 ? rows.length() : free;
	}
	
	private void removeFromFree() throws IOException {
		if(free == 0) return;
		rows.seek(free);
		long next = rows.readLong();
		rows.seek(8);
		rows.writeLong(next);
		free = next;
	}

	private void addToFree(long r) throws IOException {
		rows.seek(8);
		rows.writeLong(r);
		rows.seek(r);
		rows.writeLong(free);
		free = r;
	}
	
	public void print() throws IOException {
		for(long addr : tree.inOrder()) {
			printRow(addr);
			System.out.print('\n');
		}
	}

	private void printRow(long addr) throws IOException {
		Row r = new Row(addr);
		System.out.print("[" + addr  + " - " + r.keyField + "| ");

		for(int i = 0; i < r.otherFields.length; i++) {
			System.out.print("{");
			for(int j = 0; j < r.otherFields[i].length; j++) {
				System.out.print(r.otherFields[i][j]);
			}
			System.out.print("}");
			if(i < r.otherFields.length-1) System.out.print(", ");
		}
		System.out.print("]");
	}
	
	public void treePrint() throws IOException {
		tree.print();
	}

	public void close() throws IOException {
		tree.close();
		rows.seek(0);
		rows.writeLong(free);
		rows.writeInt(numOtherFields);
		for(int i = 0; i < numOtherFields; i++) {
			rows.writeInt(otherFieldLengths[i]);
		}
		rows.close();
	}
}
