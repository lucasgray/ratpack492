package com.ni.api.streamsforall.api.model;

import groovy.transform.Canonical
import groovy.transform.ToString

@Canonical
@ToString
public class SkinnyPost implements java.io.Serializable {

	String id
	String source
	String author
	List<String> dataview
	List<String> interest
	String sentiment
	
	String latitude
	String longitude
	
	String countryCode
	String stateCode
	String regionCode
	
	String gender
	String age
	
	
}
