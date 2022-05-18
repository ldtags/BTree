import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Stack;


public class BTree {
	private RandomAccessFile f;
	private int order;
	private int blockSize;
	private long root;
	private long free;
	
	private class Node {
		private int count;
		private int keys[];
		private long children[];
		private long address;
		
		public Node(int ct, int k[], long c[], long addr) {
			count = ct;
			keys = k;
			children = c;
			address = addr;
		}
		
		public Node(long addr) throws IOException {
			f.seek(addr);
			count = f.readInt();
			
			keys = new int[order-1];
			for(int i = 0; i < order-1; i++) {
				keys[i] = f.readInt();
			}
			
			children = new long[order];
			for(int i = 0; i < order; i++) {
				children[i] = f.readLong();
			}
			
			address = addr;
		}
		
		private void writeNode() throws IOException{
			f.seek(address);
			f.writeInt(count);
			
			for(int key : keys) {
				f.writeInt(key);
			}
			
			for(long child : children) {
				f.writeLong(child);
			}
		}
		
		private void writeNode(long r) throws IOException{
			f.seek(r);
			f.writeInt(count);
			
			for(int key : keys) {
				f.writeInt(key);
			}
			
			for(long child : children) {
				f.writeLong(child);
			}
		}
	}
	
	public BTree(String filename, int bSize) throws IOException {
		f = new RandomAccessFile(filename, "rw");
		f.seek(0);
		
		blockSize = bSize;
		f.writeInt(blockSize);
		
		order = blockSize/12;
		
		root = 0;
		free = 0;
		f.writeLong(root);
		f.writeLong(free);
	}
	
	public BTree(String filename) throws IOException {
		f = new RandomAccessFile(filename, "rw");
		f.seek(0);
		
		blockSize = f.readInt();
		order = blockSize/12;
		
		root = f.readLong();
		free = f.readLong();
	}
	
	public boolean insert(int key, long addr) throws IOException {
		//key <- key to be inserted
		//addr <- memory address of row in DBTable related to key
		if(root == 0) {	//tree is empty
			long t = getFree();
			removeFromFree();
			root = t;
			f.seek(0);
			f.writeLong(root);
			int[] rootKeys = new int[order-1];
			rootKeys[0] = key;
			long[] rootChildren = new long[order];
			rootChildren[0] = addr;
			Node root = new Node(-1, rootKeys, rootChildren, t);
			root.writeNode(t);
			return true;
		}
		
		Stack<Long> path = getPathTo(key, true);
		if(path == null) return false;
		Long r = path.pop();
		Node cur = new Node(r);
		boolean split;	//tells program if split is necessary
		int val = 0;
		long loc = 0;
		
		if(Math.abs(cur.count) < order-1) {	//checks if node is full
			sortedAdd(key, addr, cur);	//adds key and address to node cur
			split = false;
		} else {
			Node newNode = splitLeaf(cur, key, addr);
			val = newNode.keys[0];
			loc = newNode.address;
			split = true;
		}
		
		while(!path.empty() && split) {	//runs while the parent nodes need to be updated because of a split
			cur = new Node(path.pop());
			if(cur.count < order-1) {	//checks if node is full
				sortedAdd(val, loc, cur);
				split = false;
			} else {
				Node newNode = splitNode(cur, val, loc);	//get new node from split
				val = cur.keys[cur.count];	//remove new val from current node
				cur.keys[cur.count] = 0;
				cur.writeNode();
				loc = newNode.address;
			}
		}
		
		if(split) {
			long originalRoot = root;
			root = getFree();
			removeFromFree();
			f.seek(0);
			f.writeLong(root);
			int[] splitKeys = new int[order-1];
			splitKeys[0] = val;
			long[] splitChildren = new long[order];
			splitChildren[0] = originalRoot;
			splitChildren[1] = loc;
			Node newRoot = new Node(1, splitKeys, splitChildren, root);
			newRoot.writeNode();
		}
		return true;
	}

	private Node splitNode(Node cur, int key, long addr) throws IOException {
		int splitKeys[] = new int[order-1];
		long splitChildren[] = new long[order];
		int mid = cur.keys[(order-1)/2]; //getting the middle of the key array, simplifies code post split
		int i;
		if(key < mid) i = (order-1)/2;
		else i = (order-1)/2+1;
		int j = 0;
		while(i < cur.count) {	//keeping first child reference of node empty later insert
			splitKeys[j] = cur.keys[i];
			splitChildren[j+1] = cur.children[i+1];
			cur.keys[i] = 0;
			cur.children[i+1] = 0;
			i++;
			j++;
		}
		long r = getFree();
		removeFromFree();
		cur.count = i - j;
		Node newNode = new Node(j, splitKeys, splitChildren, r);
		newNode.writeNode();
		cur.writeNode();
		
		if(key > mid) {	//ensuring new val is at end of current node and the appropriate smallest node is placed in new node
			newNode.children[0] = cur.children[cur.count];
			cur.children[cur.count] = 0;
			sortedAdd(key, addr, newNode);
			cur.count--;
		} else if(key > cur.keys[cur.count-1]) {	//key is the new val being sent to the root
			cur.keys[cur.count] = key;
			newNode.children[0] = addr;
		} else {	//key < largest key in current node, so it is not the new val
			newNode.children[0] = cur.children[cur.count];
			cur.children[cur.count] = 0;
			sortedAdd(key, addr, cur);
			cur.count--;
		}
		newNode.writeNode();
		cur.writeNode();
		return newNode;
	}

	private Node splitLeaf(Node cur, int key, long addr) throws IOException {
		//cur will always be full
		int splitKeys[] = new int[order-1];
		long splitChildren[] = new long[order];
		int mid = cur.keys[order/2-1];
		int i;
		if(key < mid) i = order/2-1;	//determining how much of the node to split
		else i = order/2;
		int j = 0;
		while(i < Math.abs(cur.count)) {	//moving keys and addrs to new node
			splitKeys[j] = cur.keys[i];
			splitChildren[j] = cur.children[i];
			cur.keys[i] = 0;
			cur.children[i] = 0;
			i++;
			j++;
		}
		splitChildren[i] = cur.children[i];	//moving reference to next node to new node
		long r = getFree();
		removeFromFree();
		cur.children[i] = r;	//putting reference to new node into current node
		cur.count = -1*(i - j);
		Node newNode = new Node(-1*j, splitKeys, splitChildren, r);
		newNode.writeNode();
		cur.writeNode();
		if(key < mid) sortedAdd(key, addr, cur);	//adding new value to node dependant on where node was split
		else sortedAdd(key, addr, newNode);
		return newNode;
	}
	
	private Stack<Long> getPathTo(int key, boolean insert) throws IOException {
		Stack<Long> path = new Stack<>();	//keeps track of path to search pointer
		Node temp = new Node(root);	
		path.push(root);
		boolean searching = true;
		int i;
		while(searching) {	//while searching for node
			i = 0;
			while(i < Math.abs(temp.count) && temp.keys[i] <= key) {
				if(temp.keys[i] == key && insert && temp.count < 0) return null;	//key already exists and method is called from insert
				i++;
			}
			if(temp.count < 0) {
				searching = false;
			} else if(temp.count > 0) {
				path.push(temp.children[i]);
				temp = new Node(temp.children[i]);
			}
		}
		return path;
	}

	public long remove(int key) throws IOException {
		long keyAddr;
		boolean tooSmall = false;
		Stack<Long> path = getPathTo(key, false);
		Node cur = new Node(path.pop());

		int i = 0;	//checking to see if key is in node
		while(i < Math.abs(cur.count) && cur.keys[i] != key) i++;
		if(i == Math.abs(cur.count)) return 0;	//key is not in node
		keyAddr = cur.children[i];
		removeVal(key, cur);
		if(Math.abs(cur.count) < (order-1)/2) tooSmall = true;

		Node child;
		while(!path.empty() && tooSmall) {
			child = cur;
			cur = new Node(path.pop());
			int loc = getIndex(child.address, cur);	//loc <- index of child in cur
			Node neighbor;
			boolean right = false; //right <- true if neighbor is right neighbor of child
			if(loc == 0) {
				neighbor = new Node(cur.children[loc+1]);
				right = true;
			} else {
				neighbor = new Node(cur.children[loc-1]);
				loc--;	//setting loc to index of key in between neighbor and child
			}
			if(Math.abs(neighbor.count) > (order-1)/2) {
				//borrow
				Node rightChild = borrowVal(child, neighbor, right);
				if(child.count < 0) cur.keys[loc] = rightChild.keys[0];
				else swapVals(cur, child, right, loc);
				tooSmall = false;
			} else {
				//merge
				mergeNodes(cur, child, neighbor, right);
				if(right) staticRemoveVal(cur.keys[loc], cur);
				else removeVal(cur.keys[loc], cur);
				if(cur.count >= (order-1)/2 || (cur.address == root && cur.count > 0)) tooSmall = false;
			}
			cur.writeNode();
		}
		
		if(tooSmall) {
			root = cur.children[0];
			f.seek(0);
			f.writeLong(root);
			addToFree(cur.address);
		}
		return keyAddr;
	}

	private void mergeNodes(Node cur, Node child, Node neighbor, boolean right) throws IOException {
		//child being merged into neighbor
		int childCount = Math.abs(child.count);
		int loc = getIndex(child.address, cur);
		if(loc != 0) loc--;	//loc <- index of key in between nodes
		if(child.count > 0)	//placing key from root into child before merging
			addKey(cur.keys[loc], child);
		else if(!right)	//placing reference to next leaf node if merging left
			neighbor.children[order-1] = child.children[order-1];
		for(int i = 0; i < childCount; i++) {
			if(right) staticSortedAdd(child.keys[i], child.children[i], neighbor);
			else sortedAdd(child.keys[i], child.children[i], neighbor);
		}
		neighbor.writeNode();
	}

	private Node borrowVal(Node child, Node neighbor, boolean right) throws IOException {
		//Shifting val from neighbor into child
		//returns rightmost node
		int key;
		long addr;
		Node toReturn;
		int buf = 0;
		if(child.count > 0) buf = 1;
		int neighborCount = Math.abs(neighbor.count);
		if(right) {
			key = neighbor.keys[0];
			addr = neighbor.children[0];
			staticRemoveVal(key, neighbor);
			sortedAdd(key, addr, child);
			toReturn = neighbor;
		} else {
			key = neighbor.keys[neighborCount-1];
			addr = neighbor.children[neighborCount-1+buf];
			removeVal(key, neighbor);
			staticSortedAdd(key, addr, child);
			toReturn = child;
		}
		child.writeNode();	//updates nodes in file
		neighbor.writeNode();
		return toReturn;	//returns new key for cur
	}

	private void swapVals(Node cur, Node child, boolean right, int loc) throws IOException {
		int tempKey = cur.keys[loc];
		if(right) {
			cur.keys[loc] = child.keys[child.count-1];
			child.keys[child.count-1] = tempKey;
		} else {
			cur.keys[loc] = child.keys[0];
			child.keys[0] = tempKey;
		}
		cur.writeNode();
		child.writeNode();
	}

	private void sortedAdd(int key, long addr, Node cur) throws IOException {
		int buf = 0;
		if(cur.count > 0) buf = 1;	//creating buffer for internal nodes
		int i = Math.abs(cur.count);
		while(i > 0 && cur.keys[i-1] > key) {	//adjusting array to insert key and addr
			cur.keys[i] = cur.keys[i-1];
			cur.children[i+buf] = cur.children[i-1+buf];
			i--;
		}
		cur.keys[i] = key;	//inserting key and addr into array
		cur.children[i+buf] = addr;
		if(cur.count < 0) cur.count--;
		else cur.count++;
		cur.writeNode();
	}

	private void staticSortedAdd(int key, long addr, Node cur) throws IOException {
		//Just like staticRemoveKey, staticSortedAdd functions as a sortedAdd with no buffer for internal nodes
		int i = Math.abs(cur.count);
		if(cur.count > 0) cur.children[Math.abs(cur.count)+1] = cur.children[Math.abs(cur.count)];
		while(i > 0 && cur.keys[i-1] > key) {	//adjusting array to insert key and addr
			cur.keys[i] = cur.keys[i-1];
			cur.children[i] = cur.children[i-1];
			i--;
		}
		cur.keys[i] = key;	//inserting key and addr into array
		cur.children[i] = addr;
		if(cur.count < 0) cur.count--;
		else cur.count++;
		cur.writeNode();
	}

	private void addKey(int key, Node cur) throws IOException {
		//Adds only a key
		int i = Math.abs(cur.count);
		if(cur.count > 0) cur.keys[i+1] = cur.keys[i];
		while(i < 0 && cur.keys[i-1] > key) {
			cur.keys[i] = cur.keys[i-1];
			i--;
		}
		cur.keys[i] = key;
		if(cur.count < 0) cur.count--;
		else cur.count++;
		cur.writeNode();
	}

	private void removeVal(int key, Node cur) throws IOException {
		//Removes key and child related to key
		int i = 0;
		int buf = 0;
		if(cur.count > 0) buf = 1;
		while(i < Math.abs(cur.count)-1) {
			if(cur.keys[i] >= key) {
				cur.keys[i] = cur.keys[i+1];
				cur.children[i+buf] = cur.children[i+1+buf];
			}
			i++;
		}
		cur.keys[i] = 0;
		cur.children[i+buf] = 0;
		if(cur.count < 0) cur.count++;
		else cur.count--;
		cur.writeNode();
	}

	private void staticRemoveVal(int key, Node cur) throws IOException {
		//Unlike standard removeKey, staticRemoveKey has no buffer for internal nodes
		int i = 0;
		while(i < Math.abs(cur.count)-1) {
			if(cur.keys[i] >= key) {
				cur.keys[i] = cur.keys[i+1];
				cur.children[i] = cur.children[i+1];
			}
			i++;
		}
		cur.keys[i] = 0;
		if(cur.count < 0) cur.count++;
		else {
			cur.children[i++] = cur.children[i];
			cur.count--;
		}
		cur.children[i] = 0;
		cur.writeNode();
	}

	private int getIndex(long addr, Node cur) {
		for(int i = 0; i < cur.children.length; i++) {
			if(cur.children[i] == addr) return i;
		}
		return -1;
	}
	
	public long search(int key) throws IOException {
		return search(key, root);
	}
	
	private long search(int key, long addr) throws IOException {
		Node temp = new Node(addr);
		int i = 0;
		while(i < temp.keys.length) {
			if(key == temp.keys[i] && temp.count < 0) return temp.children[i];	//at search value and at leaf
			else if(key < temp.keys[i]) return search(key, temp.children[i]);
			else if(temp.keys[i] == 0 && key > temp.keys[i-1]) return search(key, temp.children[i]);	//node with key is in last child reference
			i++;
		}
		if(temp.count > 0) return search(key, temp.children[i]);
		return 0;	//key is not found
	}
	
	public LinkedList<Long> rangeSearch(int low, int high) throws IOException {
		LinkedList<Long> toReturn = new LinkedList<>();
		if(root == 0) return toReturn;
		Node cur = new Node(root);
		while(cur.count > 0) cur = new Node(cur.children[0]);
		int i;
		boolean done = false;
		while(!done) {
			i = 0;
			while(i < Math.abs(cur.count)) {
				if(cur.keys[i] >= low && cur.keys[i] <= high) toReturn.add(cur.children[i]);
				i++;
			}
			if(cur.children[order-1] != 0) cur = new Node(cur.children[order-1]);
			else done = true;
		}
		return toReturn;
	}
	
	private long getFree() throws IOException {
		//returns head of the free list or file length if free list is empty
		return free == 0 ? f.length() : free;
	}
	
	private void removeFromFree() throws IOException {
		//removes from head of the free list
		if(free == 0) return;
		f.seek(free);
		long next = f.readLong();
		f.seek(8);
		f.writeLong(next);
		free = next;
	}
	
	private void addToFree(long r) throws IOException {
		f.seek(8);
		f.writeLong(r);
		f.seek(r);
		f.writeLong(free);
		free = r;
	}

	public LinkedList<Long> inOrder() throws IOException {
		LinkedList<Long> toReturn = new LinkedList<>();
		if(root == 0) return toReturn;
		Node cur = new Node(root);
		while(cur.count > 0) cur = new Node(cur.children[0]);	//travel as far left as possible
		int i;
		boolean done = false;
		while(!done) {
			i = 0;
			while(i < Math.abs(cur.count)) {
				toReturn.add(cur.children[i]);
				i++;
			}
			if(cur.children[order-1] != 0) cur = new Node(cur.children[order-1]);
			else done = true;
		}
		return toReturn;
	}
	
	public void print() throws IOException {
		print(root);
	}
	
	private void print(long r) throws IOException {
		printNode(r);
		System.out.println();
		Node cur = new Node(r);
		int i = 0;
		while(i < cur.count+1) {
			print(cur.children[i]);
			i++;
		}
	}
 	
	private void printNode(long r) throws IOException {
		Node cur = new Node(r);
		int i = 0;
		System.out.print("[" + cur.address + "| (" + cur.count + ") ");
		while(i < Math.abs(cur.count)) {
			if(cur.keys[i] == 0 && i > 0) break;
			System.out.print("{" + cur.keys[i] + " - " + cur.children[i] + "}");
			if(i < cur.keys.length-1 && cur.keys[i+1] != 0) System.out.print(", ");
			i++;
		}
		System.out.print(" -> ");
		if(cur.count > 0) System.out.print(cur.children[i]);
		else System.out.print(cur.children[order-1]);	//next node references are stored at the end of the child array in leaves
		System.out.print("]");
	}
	
	public void close() throws IOException {
		f.seek(0);
		f.writeInt(blockSize);
		f.writeLong(root);
		f.writeLong(free);
		f.close();
	}
}