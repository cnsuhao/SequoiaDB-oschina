var help = function( val ) {
   if ( val == undefined )
   {
      println("   var db = new Sdb()                     connect to database use default host 'localhost' and default port 11810");
      println("   var db = new Sdb('localhost',11810)    connect to database use specified host and port");
      println("   var db = new Sdb('ubuntu',11810,\'\',\'\') connect to database with username and password");
      println("   var oma = new Oma()                    connect to om agent use default host 'localhost' and default port 11810") ;
      println("   var oma = new Oma('localhost',11810)   connect to om agent use specified host and port") ;
      println("   var oma = new Oma('ubuntu',11810,\'\',\'\') connect to om agent with username and password") ;
      println("   help(<method>)                         help on specified method, e.g. help(\'createCS\')");
      println("   oma.help()                             help on om methods");
      println("   db.help()                              help on db methods");
      println("   db.cs.help()                           help on collection space cs");
      println("   db.cs.cl                               access collection cl on collection space cs");
      println("   db.cs.cl.help()                        help on collection cl");
      println("   db.cs.cl.find()                        list all records");
      println("   db.cs.cl.find({a:1})                   list records where a=1");
      println("   db.cs.cl.find().help()                 help on find methods");
      println("   db.cs.cl.count().help()                help on count methods");
      println("   print(x), println(x)                   print out x");
      println("   sleep(ms)                              sleep macro seconds");
      println("   traceFmt(<type>,<in>,<out>)            format trace input(in) to output(out) by type");
      println("   getErr(ret)                            print error description for return code");
      println("   getLastError()                         get last error number");
      println("   setLastError(<errno>)                  set last error number") ;
      println("   getLastErrMsg()                        get last error detail information");
      println("   setLastErrMsg(<msg>)                   set last error detail information");
      println("   clear                                  clear the terminal screen");
      println("   history -c                             clear the history");
      println("   quit                                   exit");
   }
   else
   {
      man( "", val ) ;
   }
}

Sdb.prototype.help = function( val ) {
   if ( val == undefined )
   {
      println("DB methods:") ;
      println("   db.help(<method>)           help on specified method of db, e.g. db.help(\'createCS\')");
      man( "db" ) ;
   }
   else
   {
      man( "db", val ) ;
   }
}



SdbNode.prototype.help = function( val ) {
   if ( val == undefined )
   {
      println("Node methods:") ;
      println("   node.help(<method>)         help on specified method of data node, e.g. node.help(\'start\')");
      man( "node" ) ;
   }
   else
   {
      man( "node", val ) ;
   }
}

SdbReplicaGroup.prototype.help = function( val ) {
   if ( val == undefined )
   {
      println("Replica group methods:") ;
      println("   rg.help(<method>)           help on specified method of replica group rg, e.g. rg.help(\'getDetail\')");
      man( "rg" ) ;
   }
   else
   {
      man( "rg", val ) ;
   }
}

SdbCS.prototype.help = function( val ) {
   if ( val == undefined )
   {
      println("Collection Space methods:") ;
      println("   db.cs.help(<method>)        help on specified method of collection space cs, e.g. db.foo.help(\'createCL\')");
      man( "cs" ) ;
   }
   else
   {
      man( "cs", val ) ;
   }
}

SdbCollection.prototype.help = function( val ) {
   if ( val == undefined )
   {
      println("Collection methods:") ;
      println("   db.cs.cl.help(<method>)     help on specified method of collection cl, e.g. db.foo.bar.help(\'find\')");
      man( "cl" ) ;
   }
   else
   {
      man( "cl", val ) ;
   }
}

SdbCursor.prototype.help = function( val ) {
   if ( val == undefined )
   {
      println("Cursor methods:") ;
      println("   db.cs.cl.find()._exe().help(<method>) help on cursor methods") ;
	  // use query_curs
      man( "query_curs" ) ;
   }
   else
   {
      man( "query_curs", val ) ;
   }
}

SdbQuery.prototype.help = function( val ) {
   if ( val == undefined )
   {
      println("find() methods:") ;
      println("   db.cs.cl.find().help(<method>) help on find methods") ;
      println("   --methods for modifiers:") ;
      man( "query_cond" ) ;
      println("   --methods for cursor:") ;
      man( "query_curs" ) ;
      println("   --methods for general:") ;
      man( "query_gen" ) ;
   }
   else
   {
      man( "query", val ) ;
   }
}

Bson.prototype.help = function() {
   println("Bson methods:") ;
   println("   toObj() - convert to javascript object") ;
   println("   toJson() - convert to json string") ;
}

CLCount.prototype.help = function( val ) {
   if ( val == undefined )
   {
      println("count() methods:");
      println("   db.cs.cl.count().help(<method>) help on specified count methods");
      man( "count" ) ;
   }
   else
   {
      man( "count", val ) ;
   }
}

SdbDomain.prototype.help = function( val ) {
   if ( val == undefined )
   {
      println("Domain methods:") ;
      println("   domain.help(<method>)       help on specified method of domain, e.g. domain.help(\'alter\')");
      man( "domain" ) ;
   }
   else
   {
      man( "domain", val ) ;
   }
}
