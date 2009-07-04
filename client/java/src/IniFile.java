/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

package sphivedbcli;

import java.io.*;
import java.util.*;

public class IniFile {
	String fname;
	Vector lines = new Vector();

	public IniFile(String fname) {
		this.fname = fname;
		read();
	}

	public void read() {
		lines.clear();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(fname));
			while (reader.ready()) {
				String str = reader.readLine();
				lines.add(str);
				//System.out.println(str);
			}
			reader.close();
		}
		catch (FileNotFoundException e) {  }
		catch (Exception e) { e.printStackTrace();}
	}

	public void flush() {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
			for (int i=0; i<lines.size();i++) {
				String str = (String) lines.elementAt(i);
				writer.write(str);
				writer.newLine();
			}
			writer.close();
		}
		catch (Exception e) { e.printStackTrace(); }
	}

	protected String get(String section, String key) {
		boolean insideSection = false;
		String sectionstr = "["+section+"]";
		for (int i=0; i<lines.size();i++) {
			String str = (String) lines.elementAt(i);
			if (insideSection) {
				int equal = str.indexOf ( '=' );
				int comma = str.indexOf ( ';' );
				if ( comma < 0 ) comma = str.length ( );
				if (equal > 0 && comma > equal){
					if (str.substring(0, equal).trim().equals(key)){
						return str.substring(equal+1,comma).trim();
					}
				}
				if (str.startsWith("[")) return null;
			}
			else {
				if (str.compareTo(sectionstr)==0) {
					insideSection = true;
				}
			}
		}
		return null;
	}

	public void set(String section, String key, String value) {
		boolean insideSection = false;
		String sectionstr = "["+section+"]";
		String keystr = key + "=";
		int keystrlen = keystr.length();
		for (int i=0; i<lines.size(); i++) {
			String str = (String) lines.elementAt(i);
			if (insideSection) {				
				if (str.length()>= keystrlen && 
							str.substring(0,keystrlen).compareTo(keystr)==0) {
					lines.set(i, keystr+value);	// replace
					return;
				}
				if (str.length()>0 && str.charAt(0)=='[') {
					lines.add(i, keystr+value);	// insert prev
					return;
				}
			}
			else {
				if (str.compareTo(sectionstr)==0) {
					insideSection = true;
				}
			}
		}
		if (! insideSection) {
			lines.add(sectionstr);
		}
		lines.add(keystr+value);
	}
	public void clearSection(String section) {
		boolean insideSection = false;
		String sectionstr = "["+section+"]";
		for (int i=0; i<lines.size();) {
			String str = (String) lines.elementAt(i);
			if (insideSection) {
				if(str.charAt(0)=='[') return;
				lines.remove(i);
			}
			else {
				if (str.compareTo(sectionstr)==0) {
					insideSection = true;
					lines.remove(i);
				}
				else i++;
			}
		}
	}
	public void clearKey(String section, String key) {
		boolean insideSection = false;
		String sectionstr = "["+section+"]";
		String keystr = key + "=";
		int keystrlen = keystr.length();
		for (int i=0; i<lines.size(); i++) {
			String str = (String) lines.elementAt(i);
			if (insideSection) {
				if (str.substring(0,keystrlen).compareTo(keystr)==0) {
					lines.remove(i);
					return;
				}
				if (str.charAt(0)=='[') {
					return;
				}
			}
			else {
				if (str.compareTo(sectionstr)==0) {
					insideSection = true;
				}
			}
		}
	}


	public void dump() {
		for (int i=0; i<lines.size(); i++) {
			String str = (String) lines.elementAt(i);
			System.out.println(str);
		}
	}


	public int getInt(String section, String key, int defvalue) {
		String str = get(section,key);
		if (str == null) return defvalue;
		return Integer.parseInt(str);
	}

	public double getDouble(String section, String key, double defvalue) {
		String str = get(section,key);
		if (str == null) return defvalue;
		return Double.parseDouble(str);
	}

	public String getString(String section, String key, String defvalue) {
		String str = get(section,key);
		if(str == null) return defvalue;
		return str;
	}

	public void setInt(String section, String key, int value) {
		set(section, key, Integer.toString(value));
	}

	public void setDouble(String section, String key, double value) {
		set(section, key, Double.toString(value));
	}

	public void setString(String section, String key, String value) {
		set(section, key, value);
	}


	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Usage: java IniFile <inifile>");
			System.exit(-1);
		}
		IniFile ini = new IniFile(args[0]);
		String lsSection = "General", lsKey, lsValue;
		for ( int i = 0; i< 100; i++ ) {
			lsKey = "Machine" + i;
			lsValue = ini.getString(lsSection,lsKey,null);
			System.out.println(i + " " + lsValue);
		}
	}
}

