#include "parseMandocCpp.hpp"
#include "ossErr.h"
#include <string.h>


parseMandoc::parseMandoc()
{
   memset(&curp, 0, sizeof(struct curparse));
   type = MPARSE_AUTO;
   curp.outtype = OUTT_ASCII;
   curp.wlevel = MANDOCLEVEL_FATAL;
   curp.mp = mparse_alloc(type, curp.wlevel, mmsg, &curp, NULL);
   if (OUTT_MAN == curp.outtype)
      mparse_keep(curp.mp);
}

parseMandoc::~parseMandoc()
{
   if (curp.outfree)
      (*curp.outfree)(curp.outdata);
   if (curp.mp)
      mparse_free(curp.mp);
}

parseMandoc& parseMandoc::getInstance()
{
   static parseMandoc _instance ;
   return _instance ;
}

INT32 parseMandoc::parse(const CHAR* filename)
{
   INT32 rc = SDB_OK ;
   enum mandoclevel ret = MANDOCLEVEL_OK;
   ::parse(&curp, -1, filename, &ret);
   if  ( ret != MANDOCLEVEL_OK )
   {
      rc = SDB_INVALIDARG ;
   }
   return rc ;
}

