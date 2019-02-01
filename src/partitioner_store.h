//============================================================================
// Name        : RDFDataEncoder
// Version     :
// Copyright   : KAUST-Infocloud
// Description : Hello World in C++, Ansi-style
//============================================================================

#ifndef PARTITIONER_STORE_H_
#define PARTITIONER_STORE_H_
#include "utils.h"
#include "profiler.h"
#include "row.h"
#include "TurtleParser.hpp"
#include "sqlite3.h"
using namespace std;

struct triple{
	triple(){}
	triple(ll s, ll p, ll o, Type::ID _ot){
		subject = s;
		predicate = p;
		object = o;
		ot = _ot;
	}
	ll subject;
	ll predicate;
	ll object;
	Type::ID ot;
	string print(){
		//if(ot == 1){
		//	return "<"+toString(subject)+"> <"+toString(predicate)+"> \""+toString(object)+"\" .";
		//}
		//else{
			return toString(subject)+" "+toString(predicate)+" "+toString(object);
		//}
	}
};

class partitioner_store {
public:
	partitioner_store();
	virtual ~partitioner_store();
	void load_encode_rdf_data(string input_dir, string output_file_name);
	boost::unordered_map<string, ll> so_map;
	vector<sqlite3_stmt*> inserts;
	boost::unordered_map<string, ll> predicate_map;
	void dump_encoded_data(ofstream &output_stream, vector<triple> & data);
	ll total_data_size;
protected:
	void dump_dictionaries(string file_name);
	void addBatch(char const* key,ll value, sqlite3_stmt *res);
	void saveDictionaryToDisk(boost::unordered_map<string,ll> my_map, sqlite3 *db, int dictionary);
	void createNextPropertyTable(sqlite3 *dbExt) ;
};

#endif /* DATASTORE_H_ */
