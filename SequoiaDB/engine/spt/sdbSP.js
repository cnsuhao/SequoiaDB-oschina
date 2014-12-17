
// BSONObj
BSONObj.prototype.toObj = function() {
   return JSON.parse( this.toJson() ) ;
}

BSONObj.prototype.toString = function() {
   try
   {
      var obj = this.toObj() ;
      var str = JSON.stringify( obj, undefined, 2 ) ;
      return str ;
   }
   catch( e )
   {
      return this.toJson() ;
   }
}
// end BSONObj

// Oma
Oma.prototype.help = function( val ) {
   if ( val == undefined )
   {
      println("OMA methods:") ;
      println("   oma.help(<method>)          help on specified method of oma, e.g. oma.help(\'createData\')");
      man( "oma" ) ;
   }
   else
   {
      man( "oma", val ) ;
   }
}
// end Oma
