#ifndef RCGen_H
#define RCGen_H

#include <string>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/foreach.hpp>

// name string align
#define RCALIGN 32
// xml source file
#define RCXMLSRC "rclist.xml"
// output file path
#define CPPPATH "../../SequoiaDB/engine/oss/ossErr.cpp"
#define CPATH "../../SequoiaDB/engine/include/ossErr.h"
#define CSPATH "../../driver/C#.Net/Driver/exception/Errors.cs"
#define JAVAPATH "../../driver/java/src/main/resources/errors.properties"
#define JSPATH "../../SequoiaDB/engine/spt/error.js"
#define WEBPATH "../../client/admin/admintpl/error_"
#define PYTHONPATH "../../driver/python/pysequoiadb/err.prop"
#define WEBPATHSUFFIX ".php"
// chinese version document
#define DOCPATH "../../doc/references/exceptionmapping/topics/exceptionmapping_"
#define DOCPATHSUFFIX ".dita"
// XML element
#define CONSLIST "rclist.conslist"
#define CODELIST "rclist.codelist"
#define NAME "name"
#define VALUE "value"
#define DESCRIPTION "description"


class RCGen
{
    const char* language;
    std::vector<std::pair<std::string, int> > conslist;
    std::vector<std::pair<std::string, std::string> > codelist;
    void loadFromXML ();
    void genC ();
    void genCPP ();
    void genCS ();
    void genJava ();
    void genJS () ;
    void genPython() ;
public:
    RCGen (const char* lang);
    void run ();
    void genDoc () ;
    void genWeb () ;
};
#endif
