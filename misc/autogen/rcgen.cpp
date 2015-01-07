#include "rcgen.h"
#include <fstream>
#include <iostream>
#include <sstream>

using namespace boost::property_tree;
using std::cout;
using std::endl;
using std::setw;
using std::string;
using std::vector;
using std::pair;
using std::ofstream;

RCGen::RCGen (const char* lang) : language (lang)
{
    loadFromXML ();
}

void RCGen::run ()
{
    genC();
    genCPP();
    genCS();
    genJava();
    genJS();
    genPython();
}

void RCGen::loadFromXML ()
{
    ptree pt;

    try
    {
        read_xml (RCXMLSRC, pt);
    }
    catch ( std::exception& )
    {
        cout<<"Can not read xml file, not exist or wrong directory!"<<endl;
        exit(0);
    }

    try
    {
        BOOST_FOREACH (ptree::value_type &v, pt.get_child (CONSLIST))
        {
            pair<string, int> constant (
                v.second.get<string> (NAME),
                v.second.get<int> (VALUE)
            );
            conslist.push_back (constant);
        }

        BOOST_FOREACH (ptree::value_type &v, pt.get_child (CODELIST))
        {
            ptree vv = v.second.get_child(DESCRIPTION);
            pair<string, string> code (
                v.second.get<string> (NAME),
                vv.get<string> (language)
            );
            codelist.push_back (code);
        }
    }
    catch ( std::exception&)
    {
        cout<<"XML format error, unknown node name or description language, please check!"<<endl;
        exit(0);
    }
}

void RCGen::genC ()
{
    ofstream fout(CPATH);
    if ( fout == NULL )
    {
        cout<<"can not open file: "<<CPATH<<endl;
        exit(0);
    }
    string comment =
        "/** \\file ossErr.h\n"
        "    \\brief The meaning of the error code.\n"
        "*/\n"
        "/*    Copyright 2012 SequoiaDB Inc.\n"
        " *\n"
        " *    Licensed under the Apache License, Version 2.0 (the \"License\");\n"
        " *    you may not use this file except in compliance with the License.\n"
        " *    You may obtain a copy of the License at\n"
        " *\n"
        " *    http://www.apache.org/licenses/LICENSE-2.0\n"
        " *\n"
        " *    Unless required by applicable law or agreed to in writing, software\n"
        " *    distributed under the License is distributed on an \"AS IS\" BASIS,\n"
        " *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
        " *    See the License for the specific language governing permissions and\n"
        " *    limitations under the License.\n"
        " */\n"
        "/*    Copyright (C) 2011-2014 SequoiaDB Ltd.\n"
        " *    This program is free software: you can redistribute it and/or modify\n"
        " *    it under the term of the GNU Affero General Public License, version 3,\n"
        " *    as published by the Free Software Foundation.\n"
        " *\n"
        " *    This program is distributed in the hope that it will be useful,\n"
        " *    but WITHOUT ANY WARRANTY; without even the implied warrenty of\n"
        " *    MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the\n"
        " *    GNU Affero General Public License for more details.\n"
        " *\n"
        " *    You should have received a copy of the GNU Affero General Public License\n"
        " *    along with this program. If not, see <http://www.gnu.org/license/>.\n"
        " */\n";
    fout<<std::left<<comment<<endl;
    comment = "\n// This Header File is automatically generated, you MUST NOT modify this file anyway!\n"
              "// On the contrary, you can modify the xml file \"sequoiadb/misc/autogen/rclist.xml\" if necessary!\n";
    fout<<comment<<endl;

    fout<<"#ifndef OSSERR_H_"<<endl
        <<"#define OSSERR_H_"<<endl<<endl
        <<"#include \"core.h\""<<endl
        <<"#include \"ossFeat.h\""<<endl<<endl;

    for (int i = 0; i < conslist.size(); ++i)
    {
        fout<<"#define "<<setw(RCALIGN)<<conslist[i].first<<conslist[i].second<<endl;
    }
    fout<<endl;

    comment =
        "/** \\fn CHAR* getErrDesp ( INT32 errCode )\n"
        "    \\brief Error Code.\n"
        "    \\param [in] errCode The number of the error code\n"
        "    \\returns The meaning of the error code\n"
        " */";
    fout<<comment<<endl;
    fout<<"const CHAR* getErrDesp ( INT32 errCode );"<<endl<<endl;

    for (int i = 0; i < codelist.size(); ++i)
    {
        fout<<"#define "
            <<setw(RCALIGN)<<codelist[i].first
            <<setw(6)<<-(i+1)
            <<"/**< "<<codelist[i].second<<" */"<<endl;
    }

    fout<<"#endif /* OSSERR_HPP_ */";

    fout.close();
}

void RCGen::genCPP ()
{
    ofstream fout(CPPPATH);
    if ( fout == NULL )
    {
        cout<<"can not open file: "<<CPPPATH<<endl;
        exit(0);
    }

    string comment =
        "/*    Copyright 2012 SequoiaDB Inc.\n"
        " *\n"
        " *    Licensed under the Apache License, Version 2.0 (the \"License\");\n"
        " *    you may not use this file except in compliance with the License.\n"
        " *    You may obtain a copy of the License at\n"
        " *\n"
        " *    http://www.apache.org/licenses/LICENSE-2.0\n"
        " *\n"
        " *    Unless required by applicable law or agreed to in writing, software\n"
        " *    distributed under the License is distributed on an \"AS IS\" BASIS,\n"
        " *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
        " *    See the License for the specific language governing permissions and\n"
        " *    limitations under the License.\n"
        " */\n"
        "/*    Copyright (C) 2011-2014 SequoiaDB Ltd.\n"
        " *    This program is free software: you can redistribute it and/or modify\n"
        " *    it under the term of the GNU Affero General Public License, version 3,\n"
        " *    as published by the Free Software Foundation.\n"
        " *\n"
        " *    This program is distributed in the hope that it will be useful,\n"
        " *    but WITHOUT ANY WARRANTY; without even the implied warrenty of\n"
        " *    MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the\n"
        " *    GNU Affero General Public License for more details.\n"
        " *\n"
        " *    You should have received a copy of the GNU Affero General Public License\n"
        " *    along with this program. If not, see <http://www.gnu.org/license/>.\n"
        " */\n";
    fout<<comment<<endl;
    comment = "\n// This Header File is automatically generated, you MUST NOT modify this file anyway!\n"
              "// On the contrary, you can modify the xml file \"sequoiadb/misc/rcgen/rclist.xml\" if necessary!\n";
    fout<<comment<<endl;

    fout<<"#include \"ossErr.h\""<<endl<<endl;
    fout<<"const CHAR* getErrDesp ( INT32 errCode )"<<endl
        <<"{"<<endl
        <<"    INT32 code = -errCode;"<<endl
        <<"    const static CHAR* errDesp[] ="<<endl
        <<"    {"<<endl
        <<"                   \"Succeed\","<<endl;

    int size = (int)codelist.size() - 1;
    for (int i = 0; i < size; ++i)
    {
        fout<<"                   "
            <<"\""<<codelist[i].second<<"\""
            <<","<<endl;
    }
    fout<<"                   "
        <<"\""<<codelist[size].second<<"\""<<endl
        <<"    };"<<endl
        <<"    if ( code < 0 || (UINT32)code >= (sizeof ( errDesp ) / "
        <<"sizeof ( CHAR* )) )"<<endl
        <<"        return \"unknown error\";"<<endl
        <<"    return errDesp[code];"<<endl
        <<"}"<<endl;

    fout.close();
}

void RCGen::genCS ()
{
    ofstream fout(CSPATH);
    if ( fout == NULL )
    {
        cout<<"can not open file: "<<CSPATH<<endl;
        exit(0);
    }

    fout<<std::left
        <<"namespace SequoiaDB"<<endl
        <<"{"<<endl
        <<"    class Errors"<<endl
        <<"    {"<<endl
        <<"        public enum errors : int"<<endl
        <<"        {"<<endl;

    int size = (int)codelist.size() - 1;
    for (int i = 0; i < size; ++i)
    {
        fout<<"            "
            <<setw(RCALIGN)<<codelist[i].first
            <<" = "
            <<-(i+1)
            <<","<<endl;
    }
    fout<<"            "
        <<setw(RCALIGN)<<codelist[size].first
        <<" = "
        <<-(size+1)
        <<endl;

    fout<<"        };"<<endl
        <<endl
        <<"        public static readonly string[] descriptions = {"<<endl;

    for (int i = 0; i < size; ++i)
    {
        fout<<"                                                    "
            <<"\""<<codelist[i].second<<"\""
            <<","<<endl;
    }
    fout<<"                                                    "
        <<"\""<<codelist[size].second<<"\""<<endl;

    fout<<"                                                };"<<endl
        <<"    }"<<endl
        <<"}";

    fout.close();
}

void RCGen::genJava ()
{
    ofstream fout(JAVAPATH);
    if ( fout == NULL )
    {
        cout<<"can not open file: "<<JAVAPATH<<endl;
        exit(0);
    }

    fout<<std::left;
    for (int i = 0; i < codelist.size(); ++i)
    {
        fout<<setw(RCALIGN)<<codelist[i].first
            <<" = "
            <<setw(6)<<-(i+1)
            <<": "<<codelist[i].second<<endl;
    }

    fout.close();
}

void RCGen::genPython ()
{
   ofstream fout(PYTHONPATH);
   if ( fout == NULL )
   {
      cout << "can not open file: " << PYTHONPATH << endl;
      exit(0);
   }

   fout << "[error]" << endl ;
   int size = (int)codelist.size() ;
   for ( int idx = 0 ; idx < size; ++idx )
   {
      fout << std::left << setw(5) << -( idx + 1 )
           << "= " << codelist[idx].second << endl ;
   }
   fout.close();
}

string& replace_all ( string &str, const string& old_value, const string &new_value )
{
   for ( string::size_type pos(0) ; pos != string::npos; pos += new_value.length() )
   {
      if ( ( pos = str.find ( old_value, pos ) ) != string::npos )
         str.replace ( pos, old_value.length(), new_value ) ;
      else break ;
   }
   return str ;
}

void RCGen::genWeb ()
{
   string docpath = string ( WEBPATH ) + string ( language ) + string ( WEBPATHSUFFIX ) ;
   ofstream fout ( docpath.c_str() ) ;
   if ( fout == NULL )
   {
      cout << "can't open file: " << docpath << endl ;
      exit ( -1 ) ;
   }

   fout << std::left ;
   fout << "<?php" << endl ;
   fout << "$errno_" << language << " = array(" << endl ;
   for ( int i = 0; i < codelist.size(); ++i )
   {
      string first = codelist[i].first ;
      string second = codelist[i].second ;
      // replace all "$" to "\$" for web
      first = replace_all ( first, "$", "\\$" ) ;
      second = replace_all ( second, "$", "\\$" ) ;
      fout << setw(6) << -(i+1) << " => \"" << first << ": " << second << "\"" ;
      if ( i < codelist.size()-1 )
      {
         fout << "," ;
      }
      fout << endl ;
   }
   fout << ") ;" << endl ;
   fout << "?>" << endl ;
   fout.close () ;
}

void RCGen::genJS ()
{
   ofstream fout ( JSPATH ) ;
   if ( fout == NULL )
   {
      cout << "can't open file: " << JSPATH << endl ;
      exit (-1) ;
   }

   fout << std::left ;

   fout << "/* Error Constants */" << endl ;
   for ( int i = 0 ; i < conslist.size() ; i++ )
   {
      fout << "var " << setw(RCALIGN) << conslist[i].first << " = "
         << setw(6) << conslist[i].second << ";" << endl ;
   }
   fout << endl ;

   fout << "/* Error Codes */" << endl ;
   for ( int i = 0 ; i < codelist.size() ; i++ )
   {
      fout << "var " << setw(RCALIGN) << codelist[i].first << " = "
         << setw(6) << -(i + 1) << "; // "
         << codelist[i].second << ";" << endl ;
   }
   fout << endl ;

   fout << "function _getErr (errCode) {" << endl ;
   fout << "   var errDesp = [ " << endl ;
   fout << "                   \"Succeed\"," << endl ;
   for ( int i = 0 ; i < codelist.size() ; i++ )
   {
      fout << "                   \"" << codelist[i].second
         << ((i == codelist.size() - 1) ? "\"" : "\",") << endl ;
   }
   fout << "   ]; " << endl ;
   fout << "   var index = -errCode ;" << endl ;
   fout << "   if ( index < 0 || index >= errDesp.length ) " << endl ;
   fout << "      return \"unknown error\"" << endl ;
   fout << "   return errDesp[index] ;" << endl ;
   fout << "}" << endl ;
   fout << "function getErr (errCode) {" << endl ;
   fout << "   return _getErr ( errCode ) ;" << endl ;
   fout << "}" << endl ;

   fout.close() ;
}

void RCGen::genDoc ()
{
   string docpath = string ( DOCPATH ) + string ( language ) + string ( DOCPATHSUFFIX ) ;
   ofstream fout ( docpath.c_str() ) ;
   if ( fout == NULL )
   {
      cout << "can't open file: " << docpath << endl ;
      cout << "please ignore this error if it's github build" << endl ;
      // return instead of exit with -1
      // because in github build we don't have doc anymore
      return ;
      // exit (-1) ;
   }

   fout << std::left ;

   fout << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << endl ;
   fout << "<!DOCTYPE topic PUBLIC \"-//OASIS//DTD DITA Topic//EN\" \"topic.dtd\">" << endl ;
   fout << "<topic id=\"references_exception\">" << endl ;
   fout << "   <title>Error Code List</title>" << endl ;
   fout << "   <body>" << endl ;
   fout << "      <simpletable frame=\"all\" relcolwidth=\"5.25* 1.0*\" id=\"references_exceptionmapping_table\">" << endl ;
   fout << "         <sthead>" << endl ;
   fout << "            <stentry>Description</stentry>" << endl ;
   fout << "            <stentry>Error Code</stentry>" << endl ;
   fout << "         </sthead>" << endl ;
   for ( int i = 0 ; i < codelist.size() ; i++ )
   {
      fout << "         <strow>" << endl ;
      fout << "            <stentry>" << codelist[i].second << "</stentry>" << endl ;
      fout << "            <stentry>" << -(i+1) << "</stentry>" << endl ;
      fout << "         </strow>" << endl ;
   }
   fout << "      </simpletable>" << endl ;
   fout << "   </body>" << endl ;
   fout << "</topic>" << endl ;
}
