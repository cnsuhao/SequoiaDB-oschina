#include "dbConfForWeb.h"
#include "core.hpp"
#include "ossUtil.h"
#include <fstream>
#include <iostream>
#include <sstream>

#define OPTLISTTAG            "optlist"
#define LONGTAG               "long"
#define SHORTTAG              "short"
#define TYPEOFWEBTAG          "typeofweb"
#define HIDDENTAG             "hidden"
#define DETAILTAG             "detail"

#define OPTOTHERINFOFORWEBTAG "optOtherInfoForWeb"
#define TOPICTAG              "topic"
#define TITLETAG              "title"
#define BODYTAG               "body"
#define SECTIONTAG            "section"
#define SUBTITLETAG           "subtitle"
#define STEMTRYTAG            "stentry"
#define TOPIC_ATTRTAG         "topic.<xmlattr>.id"
#define XML_COMMENTTAG        "<xmlcomment>"
#define TOPIC_ATTR            "administration_database_runtime"
#define SIMPLETABLETAG        "simpletable"
#define STHEADTAG             "sthead"
#define STROWTAG              "strow"
#define STENTRY_NAMETAG       "stentry_name"
#define STENTRY_ACRONYMTAG    "stentry_acronym"
#define STENTRY_TYPETAG       "stentry_type"
#define STENTRY_DESTTAG       "stentry_dest"
#define NOTETAG               "note"
#define NOTE_FIRSTTAG         "first"
#define NOTE_SECONDTAG        "second"
#define PTAG                  "p"
#define XMLDECLARATION        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
#define XMLDTD                "<!DOCTYPE topic PUBLIC \"-//OASIS//DTD DITA Topic//EN\" \"topic.dtd\">"
#define XMLCOMMENT            "id=\"runtime_table\" frame=\"all\" relcolwidth=\"1.43* 1.0*1.18*11.41*\""

using namespace boost::property_tree;
using std::cout;
using std::endl;
using std::size_t;
using std::setw;
using std::string;
using std::vector;
using std::ofstream;
using std::ostringstream;

const CHAR *pLanguage[] = { "en", "cn" } ;

OptGenForWeb::OptGenForWeb ( const char* lang ) : language( lang )
{
    loadFromXML () ;
    loadOtherInfoFromXML () ;
}

OptGenForWeb::~OptGenForWeb ()
{
    vector<OptEle*>::iterator it ;
    vector<OptOtherInfoEle*>::iterator ite ;
    for ( it = optlist.begin(); it != optlist.end(); it++ )
    {
        delete *it ;
    }
    optlist.clear() ;
    for ( ite = optOtherInfo.begin(); ite != optOtherInfo.end(); ite++ )
    {
        delete *ite ;
    }
    optOtherInfo.clear() ;
}

void OptGenForWeb::loadOtherInfoFromXML ()
{
    ptree pt ;
    try
    {
       read_xml ( OPTOTHERINFOFORWEBFILE, pt ) ;
    }
    catch ( std::exception &e )
    {
        cout << "Can not read xml file, not exist or wrong directory forOptGenForWeb: "
             << e.what() << endl ;
        exit ( 0 ) ;
    }

    OptOtherInfoEle *newele = new OptOtherInfoEle() ;
    if ( !newele )
    {
        cout << "Failed to allocate memory for OptOtherInfoEle!" << endl ;
        exit ( 0 ) ;
    }

    try
    {
        BOOST_FOREACH ( ptree::value_type &v, pt.get_child( OPTOTHERINFOFORWEBTAG ) )
        {
            if ( TITLETAG == v.first )
            {
                try
                {
                    newele->titletag = v.second.get<string>(language) ;
                }
                catch ( std::exception &e )
                {
                    cout << "Wrong to get the title tag: " << e.what()
                         << endl ;
                    continue ;
                }
            }
            else if ( SUBTITLETAG == v.first )
            {
                try
                {
                    newele->subtitletag = v.second.get<string>(language) ;
                }
                catch ( std::exception &e )
                {
                    cout << "Wrong to get the subtitle tag: " << e.what()
                         << endl ;
                    continue ;
                }
            }
            else if ( STHEADTAG == v.first )
            {
                try
                {
                    BOOST_FOREACH( ptree::value_type &v1, v.second )
                    {
                        if ( STENTRY_NAMETAG == v1.first )
                        {
                            newele->stentry_nametag = v1.second
                                           .get<string>(language) ;
                        }
                        else if ( STENTRY_ACRONYMTAG == v1.first )
                        {
                            newele->stentry_acronymtag = v1.second
                                           .get<string>(language) ;
                        }
                        else if ( STENTRY_TYPETAG == v1.first )
                        {
                            newele->stentry_typetag = v1.second
                                           .get<string>(language) ;
                        }
                        else if ( STENTRY_DESTTAG == v1.first )
                        {
                            newele->stentry_desttag = v1.second
                                           .get<string>(language) ;
                        }
                    }
                }
                catch ( std::exception &e )
                {
                    cout << "Wrong to get the stentry tags: " << e.what()
                         << endl ;
                    continue ;
                }
            }
            else if ( NOTETAG == v.first )
            {
                try
                {
                    BOOST_FOREACH( ptree::value_type &v2, v.second )
                    {
                        if ( NOTE_FIRSTTAG == v2.first )
                        {
                            newele->firsttag = v2.second
                                    .get<string>(language) ;
                        }
                        else if ( NOTE_SECONDTAG == v2.first )
                        {
                            newele->secondtag = v2.second
                                    .get<string>(language) ;
                        }
                    }
                }
                catch ( std::exception &e )
                {
                    cout << "Wrong to get the note tags: " << e.what()
                         << endl ;
                    continue ;
                }
            }
        }
    }
    catch ( std::exception& e )
    {
        cout << "XML format error, unknown node name \
or description language,please check!" << endl ;
        exit(0) ;
    }
    optOtherInfo.push_back ( newele ) ;
}

void OptGenForWeb::loadFromXML ()
{
    ptree pt ;
    try
    {
        read_xml ( OPTXMLSRCFILE, pt ) ;
    }
    catch ( std::exception &e )
    {
        cout << "Can not read src xml file, not exist or wrong directory for OptGenForWeb: "
             << e.what() << endl ;
        exit ( 0 ) ;
    }
    try
    {
        BOOST_FOREACH ( ptree::value_type &v, pt.get_child( OPTLISTTAG ) )
        {
            BOOLEAN ishidden = FALSE ;
            OptEle *newele = new OptEle() ;
            if ( !newele )
            {
                cout << "Failed to allocate memory for OptEle!" << endl ;
                exit ( 0 ) ;
            }
            try
            {
                ossStrToBoolean ( v.second.get<string>(HIDDENTAG).c_str(),
                                 &ishidden ) ;
            }
            catch ( std::exception & )
            {
                ishidden = FALSE ;
            }
            if ( ishidden )
            {
                continue ;
            }

            try
            {
                newele->longtag += v.second.get<string>(LONGTAG) ;
            }
            catch ( std::exception &e )
            {
                cout << "Long tag is requird: " << e.what()
                     << endl ;
                continue ;
            }
            try
            {
                newele->detailtag = v.second.get_child(DETAILTAG
                                           ).get<string>(language) ;
            }
            catch ( std::exception &e )
            {
                continue ;
            }
            try
            {
                newele->shorttag += v.second.get<string>(SHORTTAG) ;
            }
            catch ( std::exception &e )
            {
                newele->shorttag += "-" ;
            }
            try
            {
                newele->typeofwebtag = v.second.get<string>(TYPEOFWEBTAG) ;
            }
            catch ( std::exception &e )
            {
                newele->typeofwebtag = "--" ;
            }

            optlist.push_back ( newele ) ;
        }
    }
    catch ( std::exception& )
    {
        cout << "XML format error, unknown node name \
or description language,please check!"<<endl ;
        exit(0) ;
    }
}

string OptGenForWeb::genOptions ()
{
    ptree pt ;
    ptree topic ;
    ptree body ;
    ptree section ;
    ptree simpletable ;
    ptree note ;
    ptree sthead ;
    ptree strow ;

    vector<OptEle*>::iterator it ;
    vector<OptOtherInfoEle*>::iterator ite ;
    ostringstream oss ;

    ite = optOtherInfo.begin() ;
    if ( optOtherInfo.end() == ite )
    {
        cout << "Nothing in 'optOtherInfo'." << endl ;
        exit ( 0 ) ;
    }
/****************************
<topic>
   <title></title>
   <body>
      <section>
         <title></title>
         <sampletable>
            <sthread>
            </sthread>
            <strow>
            </strow>
               ...
         </sampletable>
         <note>
         </note>
      </section>
   </body>
</topic>
******************************/
    sthead.add ( STEMTRYTAG, (*ite)->stentry_nametag ) ;
    sthead.add ( STEMTRYTAG, (*ite)->stentry_acronymtag ) ;
    sthead.add ( STEMTRYTAG, (*ite)->stentry_typetag ) ;
    sthead.add ( STEMTRYTAG, (*ite)->stentry_desttag ) ;
    simpletable.add ( XML_COMMENTTAG, XMLCOMMENT ) ;
    simpletable.add_child ( STHEADTAG, sthead ) ;
    for ( it = optlist.begin(); it != optlist.end(); it++ )
    {
        strow.clear () ;
        strow.add ( STEMTRYTAG, (*it)->longtag ) ;
        strow.add ( STEMTRYTAG, (*it)->shorttag ) ;
        strow.add ( STEMTRYTAG, (*it)->typeofwebtag ) ;
        strow.add ( STEMTRYTAG, (*it)->detailtag ) ;
        simpletable.add_child ( STROWTAG, strow ) ;
    }
    note.add ( PTAG, (*ite)->firsttag ) ;
    note.add ( PTAG, (*ite)->secondtag ) ;
    section.add ( TITLETAG, (*ite)->subtitletag ) ;
    section.add_child ( SIMPLETABLETAG, simpletable ) ;
    section.add_child ( NOTETAG, note ) ;
    body.add_child ( SECTIONTAG, section ) ;
    topic.add ( TITLETAG, (*ite)->titletag ) ;
    topic.add_child ( BODYTAG, body ) ;
    pt.add_child ( TOPICTAG, topic ) ;
    pt.add ( TOPIC_ATTRTAG, TOPIC_ATTR ) ;
    xml_writer_settings<char> settings( '\t', 1 ) ;
    write_xml ( oss, pt, settings ) ;
    return oss.str () ;
}

void OptGenForWeb::gendoc()
{
    string str ;
    string subStr ;
    size_t found ;
    string fileName ;

    if ( 0 == strcmp( pLanguage[0], language ) )
    {
        fileName = string( DBCONFFORWEBPATH ) + string( "_en" ) + string( FILESUFFIX ) ;
    }
    else if ( 0 == strcmp( pLanguage[1], language ) )
    {
        fileName = string( DBCONFFORWEBPATH ) + string( FILESUFFIX ) ;
    }
    else
    {
        cout << "The language is not support: " << language << endl ;
    }

    str = genOptions() ;
    if ( "" == str.c_str() )
    {
        cout << "Failed to generate database configuration options." << endl ;
        exit ( 0 ) ;
    }

    ofstream fout( fileName.c_str() ) ;
    if ( NULL == fout )
    {
        cout << "Can not open file: " << fileName << endl;
        exit(0);
    }
    found = str.find_first_of ( '>' ) ;
    if ( string::npos != found )
    {
        subStr = str.substr ( 0, found+1 ) ;
        fout << subStr.c_str() << '\n' ;
        fout << XMLDTD ;
        fout << str.substr ( found+1, string::npos ) ;
    }
    else
    {
        cout << "Can not add DTD, " << "\"found\" is: " << found << endl ;
        exit ( 0 ) ;
    }
}

void OptGenForWeb::run ()
{
   gendoc () ;
}

