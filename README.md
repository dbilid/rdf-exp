### This is the modified NewFastEncoder, originally from https://github.com/ecrc/rdf-exp. This modified version saves output to SQLite tables, ready to be used by PARJ.

#### Compiling and Running
 * Change your current working directory to the NewFastEncoder directory.     
       ```
       cd Release ; make clean
       ```
       
* Compile NewFastEncoder. 
       ```
       make all
       ```
       
* Usage: 
       ```
       NewFastEncoder <input_dir> <encoded_file_path>
       ```
       
input_dir is the path of the RDF data files while encoded_file_path is the location where the encoded data will be located. Notice that the dictionaries will be created at the directory of the encoded data. 

#### License
  *  This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
