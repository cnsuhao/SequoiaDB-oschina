/*******************************************************************************

   Copyright (C) 2011-2014 SequoiaDB Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the term of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warrenty of
   MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.

*******************************************************************************/

#include <stdio.h>
#include <iostream>
#include "../spt/engine.h"
#include "../bson/bson.h"
#include "../bson/bson-inl.h"

using namespace engine;
using namespace bson;


BSONObj getTimeNative(const BSONObj &args, void* data)
{
	BSONObjBuilder b;
	int numField = args.nFields();
	if(numField>1 || (numField==1 && !args.firstElement().isNumber()))
	{
		b.append("return", "Syntax: gettime([current timestamp since 1970])");
	}
	else
	{
		char timeBuffer[50];
		time_t t;
		if(args.nFields()==0)
		{
			t = time(NULL); 
		}
		else
		{
			t = args[1].number();
		}
		struct tm tm;
#if defined (_LINUX)
		localtime_r(&t, &tm);
		b.append("return", asctime_r(&tm, timeBuffer));
#else
		localtime_s(&tm, &t);
		b.append("return", asctime_s(timeBuffer, 50, &tm));
#endif
	}
	return b.obj();

}
int main(int argc, char** argv)
{
	if(argc!=2)
	{
		printf("Syntax: %s <file name>\n", argv[0]);
		return 0;
	}
	/**********************initiailize*****************************/
	printf("** setup SMEngine\n");
	ScriptEngine::setup();

	printf("** create SMScope\n");
	Scope* s = globalScriptEngine->getPooledScope("test");
	
	/***********************load script file***********************/
	FILE *fp = fopen((char*)argv[1], "r");
	if(!fp)
	{
		printf("Failed to open file %s\n", (char*)argv[1]);
		return 0;
	}

	fseek(fp, 0, SEEK_END);
	long filesize = ftell(fp);
	fseek(fp, 0, SEEK_SET);

	char* pBuffer=(char*)malloc(sizeof(char)* (filesize+1));
	fread(pBuffer, filesize, 1, fp);
	pBuffer[filesize]=0;

	printf("** Direct execute\n");
	s->exec(pBuffer, "foo", true, true, true, 0);
	printf("** Creating function\n");
	ScriptingFunction func=s->createFunction(pBuffer);
	printf("** Function address = 0x%016llx\n", func);
	if(!s)
	{
		printf("failed to create function\n");
	}
	else
	{
		int rc=s->invoke(func, 0,0,0,false );
		if(0==rc)
		{
			BSONObj obj = s->getObject("return");
			cout<<obj;
		}
		printf("\n** rc=%d\n", rc);
	}
	fclose(fp);
	free(pBuffer);


	/********************** create native function*************************/
	printf("** Creating gettime() native function\n");
	
	if(s->injectNative("gettime", getTimeNative))
	{
		if(!s->exec("gettime()", "callNativeFunction", true, true, true, 0))
		{
			printf("** Failed to call gettime() in javascript\n");
		}
	}
	else
		printf("** Failed to inject gettime() native function\n");

	printf("** Done calling native function\n");
	/*********************** clean up ***************************************/
	printf("cleanup\n");
	delete(s);
	return 0;
}
