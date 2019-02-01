//============================================================================
// Name        : RDFDataEncoder
// Version     :
// Copyright   : KAUST-Infocloud
// Description : Hello World in C++, Ansi-style
//============================================================================

#include "partitioner_store.h"
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include "sqlite3.h"
#include <sstream>

int getdir (string dir, vector<string> &files)
{
	DIR *dp;
	struct dirent *dirp;
	if((dp  = opendir(dir.c_str())) == NULL) {
		cout << "Error(" << errno << ") opening " << dir << endl;
		return errno;
	}

	while ((dirp = readdir(dp)) != NULL) {
		if(((int)dirp->d_type) == 8)
			files.push_back(string(dirp->d_name));
	}
	closedir(dp);
	return 0;
}

partitioner_store::partitioner_store() {
}

partitioner_store::~partitioner_store() {
	/*for (unsigned i = 0; i < rdf_data.size(); i++)
		delete rdf_data[i];*/
}

void partitioner_store::load_encode_rdf_data(string input_dir, string output_file_name) {
	print_to_screen(part_string);
	Profiler profiler;
	profiler.startTimer("load_rdf_data");
	boost::unordered_map<string, ll>::iterator map_it;
	ll so_id = 1;
	ll predicate_id = 1;
	ll num_rec = 0;
	triple tmp_triple;
	ofstream ofs;
	string input_file;
	Type::ID objectType;
	ll sid, pid, oid;
	vector<triple> tmp_data;
	string subject,predicate,object,objectSubType;
	vector<string> files = vector<string>();
	
	inserts=vector<sqlite3_stmt*>();



	int rc;
        sqlite3 *dbExt;
        string sql;
        rc = sqlite3_open_v2("file::memory:", &dbExt,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI | SQLITE_OPEN_CREATE, NULL);
        if (rc != SQLITE_OK) {

                fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(dbExt));
                //sqlite3_close(db);

                return ;
        }
        sql= string("attach database '")+string("/tmp/rdf.db")+string("' as ext;");
        //strcpy(sql, "select * from  p0all;");
        rc = sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);

        if (rc != SQLITE_OK) {

                fprintf(stderr, "Cannot attach database: %s\n", sqlite3_errmsg(dbExt));
                //sqlite3_close(db);

                return ;
        }



	sql= "PRAGMA page_size = 4096;";

        sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);

        sql= "PRAGMA cache_size = 1048576;";

        sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);

        sql= "PRAGMA synchronous = OFF;";

        sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);
        sql= "PRAGMA locking_mode = EXCLUSIVE;";

        sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);
        sql= "PRAGMA journal_mode = OFF;";

        sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);
        sql= "PRAGMA temp_store=2;";
        sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);
        //createMemoryDB(dbt);
        //strcpy(sql, "detach database aa;");
        //sqlite3_exec(dbt, sql, 0, 0, NULL);
        //strcpy(sql, "PRAGMA query_only = 1;");
        sqlite3_exec(dbExt, "BEGIN", 0, 0, 0);


	getdir(input_dir,files);
	ofs.open(output_file_name.c_str(), ofstream::out);
	for (unsigned int file_id = 0; file_id < files.size();file_id++) {
		input_file = string(input_dir+"/"+files[file_id]);
		ifstream fin(input_file.c_str());
		TurtleParser parser(fin);
		print_to_screen("Reading triples from file: " + input_file);
		try {
			while (true) {
				try {
					if (!parser.parse(subject,predicate,object,objectType,objectSubType))
						break;
				} catch (const TurtleParser::Exception& e) {
					cerr << e.message << endl;
					// recover...
					while (fin.get()!='\n') ;
					continue;
				}

				//lookup subject
				map_it = so_map.find(subject);
				if (map_it == so_map.end()) {
					so_map[subject] = so_id;
					sid = so_id;
					so_id++;
				}
				else{
					sid = map_it->second;
				}

				//lookup predicate
				map_it = predicate_map.find(predicate);
				if (map_it == predicate_map.end()) {
					predicate_map[predicate] = predicate_id;
					pid = predicate_id;
					predicate_id++;
					createNextPropertyTable(dbExt) ;
					
				}
				else{
					pid = map_it->second;
				}

				//lookup object
				for(unsigned i = 0 ; i < object.length() ;i++){
					if(object[i] == '\n' || object[i] == '\r')
						object [i] = ' ';
				}
				map_it = so_map.find(object);
				if (map_it == so_map.end()) {
					//if(objectType == 1){
					//	so_map["\""+object+"\""] = so_id;
					//}
					//else
					so_map[object] = so_id;
					oid = so_id;
					so_id++;
				}
				else{
					oid = map_it->second;
				}

				sqlite3_bind_int64(inserts[pid-1], 1, sid);
				sqlite3_bind_int64(inserts[pid-1], 2, oid);
				int rc = sqlite3_step(inserts.at(pid-1));
				if (rc != SQLITE_DONE) {
					printf("Commit Failed! error:%i\n", rc);
				}
				sqlite3_reset(inserts.at(pid-1));
				//tmp_triple = triple(sid, pid, oid, objectType);
				//tmp_data.push_back(tmp_triple);
				num_rec++;
				if (num_rec % 1000000 == 0) {
					//this->dump_encoded_data(ofs, tmp_data);
					//tmp_data.clear();
					cout<<"Finished reading "<<num_rec<<endl;
				}
			}
			//this->dump_encoded_data(ofs, tmp_data);
			//tmp_data.clear();
			fin.close();
		}catch (const TurtleParser::Exception&) {
			return ;
		}
	}
	ofs.close();


	total_data_size = num_rec;
	print_to_screen(part_string+"\nTotal number of triples: " + toString(this->total_data_size) + " records");
	profiler.pauseTimer("load_rdf_data");
	print_to_screen("Done with data encoding in " + toString(profiler.readPeriod("load_rdf_data")) + " sec");
	profiler.clearTimer("load_rdf_data");
	//this->dump_dictionaries(output_file_name);


	/*int rc;
	sqlite3 *dbExt;
	string sql;
	rc = sqlite3_open_v2("file::memory:", &dbExt,
	SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI | SQLITE_OPEN_CREATE, NULL);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(dbExt));
		//sqlite3_close(db);

		return ;
	}
	sql= string("attach database '")+string("/tmp/rdf.db")+string("' as ext;");
	//strcpy(sql, "select * from  p0all;");
	rc = sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);

	if (rc != SQLITE_OK) {

		fprintf(stderr, "Cannot attach database: %s\n", sqlite3_errmsg(dbExt));
		//sqlite3_close(db);

		return ;
	}
*/
/*
	sql= "PRAGMA page_size = 4096;";

	sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);

	sql= "PRAGMA cache_size = 1048576;";

	sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);

	sql= "PRAGMA synchronous = OFF;";

	sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);
	sql= "PRAGMA locking_mode = EXCLUSIVE;";

	sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);
	sql= "PRAGMA journal_mode = OFF;";

	sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);
	sql= "PRAGMA temp_store=2;";
	sqlite3_exec(dbExt, sql.c_str(), 0, 0, NULL);
	//createMemoryDB(dbt);
	//strcpy(sql, "detach database aa;");
	//sqlite3_exec(dbt, sql, 0, 0, NULL);
	//strcpy(sql, "PRAGMA query_only = 1;");
	sqlite3_exec(dbExt, "BEGIN", 0, 0, 0);
*/
	cout << "Saving dictionaries... ";

	saveDictionaryToDisk(so_map, dbExt, 1);
	saveDictionaryToDisk(predicate_map, dbExt, 0);

	cout << "Creating inverse properties... ";	
	unsigned int pId;
	

	char create[100];
	char insert[100];
	for (pId = 1; pId < inserts.size()+1; pId++) {
		
		rc = sqlite3_finalize(inserts.at(pId-1));
		if (rc != SQLITE_OK) {

			fprintf(stderr,
					"Could not finalize dictionary/properties statement %s",
					sqlite3_errmsg(dbExt));
			//sqlite3_close(dbExt);

			return;
		}

		snprintf(create, sizeof create,
				"create table ext.invprop%i (o INTEGER, s INTEGER, primary key(o, s)) without rowid;",
				pId);
		rc = sqlite3_exec(dbExt, create, 0, 0, NULL);

		if (rc != SQLITE_OK) {

			fprintf(stderr, "Could not create inv property table: %s\n",
			sqlite3_errmsg(dbExt));
			//sqlite3_close(dbExt);

			return;
		}

		snprintf(insert, sizeof insert,
				"insert into invprop%i select o, s from prop%i; ", pId, pId);
		rc = sqlite3_exec(dbExt, insert, 0, 0, NULL);

		if (rc != SQLITE_OK) {

			fprintf(stderr, "Could not insert into inv property table: %s\n",
			sqlite3_errmsg(dbExt));
			//sqlite3_close(dbExt);

			return;
		}
	}

	cout << "Creating indexes... ";

	rc = sqlite3_exec(dbExt,
			"CREATE UNIQUE INDEX ext.uriindex on dictionary(uri);", 0, 0, NULL);

	if (rc != SQLITE_OK) {

		fprintf(stderr, "Could not create  index on dictionary %s",
		sqlite3_errmsg(dbExt));
		//sqlite3_close(dbExt);

		return;
	}
	rc = sqlite3_exec(dbExt,
			"CREATE UNIQUE INDEX ext.uriindex2 on properties(uri);", 0, 0,
			NULL);

	if (rc != SQLITE_OK) {

		fprintf(stderr, "Could not create  index on properties %s",
		sqlite3_errmsg(dbExt));
		//sqlite3_close(dbExt);

		return;
	}


	rc = sqlite3_exec(dbExt, "END", 0, 0, 0);
	if (rc != SQLITE_OK) {
		
		//fprintf(stderr, "Cannot commit: %s\n", sqlite3_errmsg(dbExt));
		sqlite3_close(dbExt);
		cout << "Error(" << rc << ") closing connection ";
		return ;
	}

	rc = sqlite3_close(dbExt);
	if (rc != SQLITE_OK) {

		//fprintf(stderr, "Cannot close ext connection: %s\n",
		//		sqlite3_errmsg(dbExt));
		sqlite3_close(dbExt);
		cout << "Error(" << rc << ") closing connection ";
		return;
	}
	cout << "Finished!";
}

void partitioner_store::dump_dictionaries(string file_name) {
	print_to_screen(part_string);
	Profiler profiler;
	profiler.startTimer("dump_dictionaries");
	print_to_screen("Dumping Dictionaries!");
	dump_map(so_map, file_name+"verts_map.dic", true);
	dump_map(predicate_map, file_name+"predicate_map.dic", true);
	profiler.pauseTimer("dump_dictionaries");
	print_to_screen("Done with dumping dictionaries in " + toString(profiler.readPeriod("dump_dictionaries")) + " sec");
	profiler.clearTimer("dump_dictionaries");

}

void partitioner_store::dump_encoded_data(ofstream &output_stream, vector<triple> & data){
	print_to_screen(part_string);
	Profiler profiler;
	profiler.startTimer("dump_encoded_data");

	for(unsigned i = 0 ; i < data.size(); i++){
		output_stream<<data[i].print()<<endl;
	}

	profiler.pauseTimer("dump_encoded_data");
	print_to_screen("Done with dump_encoded_data in "+toString(profiler.readPeriod("dump_encoded_data"))+" sec");
	profiler.clearTimer("dump_encoded_data");
}


void partitioner_store::saveDictionaryToDisk(boost::unordered_map<string, ll> my_map, sqlite3 *db, int dictionary) {
	
	string sql;
	if (dictionary) {
		sql="create table ext.dictionary(id INTEGER PRIMARY KEY, uri TEXT)";
	} else {
		sql="create table ext.properties(id INTEGER PRIMARY KEY, uri TEXT)";
	}
	int rc;
	rc = sqlite3_exec(db, sql.c_str(), 0, 0, NULL);
	if (rc != SQLITE_OK) {
		cout << "Error(" << rc << ") saving dictionary ";
		//fprintf(stderr, "could not create dictionary : %s\n",
		//sqlite3_errmsg(db));
		return;
	}
	if (dictionary) {
		sql= "insert into ext.dictionary values(?, ?)";
	} else {
		sql= "insert into ext.properties values(?, ?)";
	}

	sqlite3_stmt *res;
	rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &res, 0);
	if (rc != SQLITE_OK) {
		cout << "Error(" << rc << ") saving dictionary ";
		//fprintf(stderr, "Cannot prepare statement: %s\n", sqlite3_errmsg(db));
		return;
	}

	//GHashTableIter iter;
	//gpointer key, value;

	//g_hash_table_iter_init(&iter, hash_table);

	typename boost::unordered_map<string,unsigned int>::iterator i;
	string uri;
	ll id;
	//inverse=1;
        for(i = my_map.begin(); i != my_map.end(); i++){
          //      if(inverse){
	//		uri=toString((*i).second);
	//		id=(*i).first;
                        //mapStream<<toString((*i).second)<<" "<<toString((*i).first)<<endl;
          //      }
            //    else{
			uri=toString((*i).first);
                        id=(*i).second;

                        //mapStream<<toString((*i).first)<<" "<<toString((*i).second)<<endl;
             //  }
	addBatch(uri.c_str(), id, res);
        }
//        mapStream.close();
	//while (g_hash_table_iter_next(&iter, &key, &value)) {
		//printf("key:%p values:%p \n", key, value);
	//	char* k = (char*) key;
	//	int v = GPOINTER_TO_INT(value);
		//addBatch(uri.c_str(), id, res);
	//}

	rc = sqlite3_finalize(res);
	//free(sql);

}

void partitioner_store::addBatch(char const* key, ll value, sqlite3_stmt *res) {
	//printf("key:%s value:%d \n", key, value);
	sqlite3_bind_int(res, 1, value);
	sqlite3_bind_text(res, 2, key, -1, SQLITE_STATIC);
	if (sqlite3_step(res) != SQLITE_DONE) {
		printf("Commit Failed!\n");
	}
	sqlite3_reset(res);
}


void partitioner_store::createNextPropertyTable(sqlite3 *dbExt) {
	
	//string create=string("create table ext.prop")+boost::lexical_cast<std::string>(inserts.size()+1)+string(" (s INTEGER, o INTEGER, primary key(s, o)) without rowid;");
	std::ostringstream create;
	create << "create table ext.prop" << (inserts.size()+1) << " (s INTEGER, o INTEGER, primary key(s, o)) without rowid;";
	//char create[100];
	//snprintf(create, sizeof create, "%s%i%s", createPrefix, propId,
	//		createPostfix);
	int rc;
	rc = sqlite3_exec(dbExt, create.str().c_str(), 0, 0, NULL);

	if (rc != SQLITE_OK) {

		fprintf(stderr, "Could not create property table: %s\n",
		sqlite3_errmsg(dbExt));
		//sqlite3_close(dbExt);

		return;
	}
	//char insert[100];
	//string insert;
	std::ostringstream insert;
        insert << "insert or ignore into ext.prop" << (inserts.size()+1) << " values(?, ?) ;";
	//insert=string("insert or ignore into ext.prop"+boost::lexical_cast<std::string>(inserts.size()+1)+string(" values(?, ?) ;");
	//snprintf(insert, sizeof insert, "%s%i%s", insertPrefix, propId,
	//		insertPostfix);
	sqlite3_stmt *stmt; 
	rc = sqlite3_prepare_v2(dbExt, insert.str().c_str(), -1, &stmt, 0);
	if (rc != SQLITE_OK) {

		fprintf(stderr, "Cannot prepare statement: %s\n",
		sqlite3_errmsg(dbExt));
		//sqlite3_close(dbExt);

		return;
	}
	inserts.push_back(stmt);
}
