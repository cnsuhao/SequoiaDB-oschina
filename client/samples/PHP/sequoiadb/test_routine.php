<?php
/*
 *  In PHP, single quotation are not the pass of the compiler analysis,
 *  but double quotes are compiler analysis.
 */

// create a new connection object
echo '<p>1. connect to server</p>' ;
$sdb = new SequoiaDB ("localhost:50000") ;
var_dump ( $sdb->getError() ) ;

if ( $sdb )
{
   // create a new collection space object
   echo '<p>2. create collection space foo</p>' ;
   $cs = $sdb->selectCS ( "foo" ) ;
   var_dump ( $sdb->getError() ) ;
   if ( $cs )
   {
      // create a new collection object
      echo '<p>3. create collection test</p>' ;
      $cl = $cs->selectCollection ( "test" ) ;
      if ( $cl )
      {
         // create a new list collection space cursor object
         echo '<p>4. list collection spaces</p>' ;
         $sdb_cursor = $sdb->listCSs () ;
         if ( $sdb_cursor )
         {
            while ( $arr = $sdb_cursor->getNext () )
            {
               var_dump ( $arr ) ;
               echo "<br><br>" ;
            }
         }
         
         // rename this collection name
         echo '<p>5. rename this collection name</p>' ;
         var_dump ( $cl->rename ( "big" ) ) ;
         echo "<br><br>" ;

         // create a new list collections cursor object
         echo '<p>6. list collections</p>' ;
         $sdb_cursor = $sdb->listCollections () ;
         if ( $sdb_cursor )
         {
            // if you want to print sting,you can set install = false
            echo '<p>7. reset this cursor print mode string</p>' ;
            $sdb_cursor->install ( array ( 'install' => false ) ) ;
            while ( $str = $sdb_cursor->getNext () )
            {
               echo $str ;
               echo "<br><br>" ;
            }
         }
         
         // insert record array
         echo '<p>8. insert array</p>' ;
         $arr = array ( 'a' => 1, 'b' => new SequoiaDate('2012-12-21') ) ;
         var_dump ( $cl->insert ( $arr ) ) ;
         echo "<br><br>" ;
         
         $str = '{"a":1,"b":{"$date":"2012-12-21"}}' ;
         echo '<p>9. insert string '.$str.' </p>' ;
         // insert record string
         var_dump ( $cl->insert ( $str ) ) ;
         echo "<br><br>" ;
         
         // update all record
         echo '<p>10. update record</p>' ;
         var_dump ( $cl->update ( array ( '$set' => array ( 'c' => 'hello world' ) ) ) ) ;
         echo "<br><br>" ;
         
         // count from collection
         echo '<p>11. count from collection</p>' ;
         echo $cl->count () ;
         echo " records<br><br>" ;
         
         echo '<p>12. query from collection</p>' ;
         // query from collection
         $cl_cursor = $cl->find() ;
         while ( $arr = $cl_cursor->getNext () )
         {
            var_dump ( $arr ) ;
            echo "<br><br>" ;
         }
         
         $arr = array ( "a" => 1 ) ;
         echo '<p>13. create index</p>' ;
         // create index
         var_dump ( $cl->createIndex ( $arr, 'myIndex' ) ) ;
         echo "<br><br>" ;
         
         echo '<p>14. query index</p>' ;
         // query index
         $index_cursor = $cl->getIndex ( 'myIndex' ) ;
         while ( $arr = $index_cursor->getNext () )
         {
            var_dump ( $arr ) ;
            echo "<br><br>" ;
         }
         
         echo '<p>15. delete index</p>' ;
         // delete index
         var_dump ( $cl->deleteIndex ( 'myIndex' ) ) ;
         echo "<br><br>" ;
         
         echo '<p>16. get name</p>' ;
         echo 'collection space name: '. $cl->getCSName() ;
         echo "<br><br>" ;
         echo 'collection name: '. $cl->getCollectionName() ;
         echo "<br><br>" ;
         echo 'collection full name: '. $cl->getFullName() ;
         echo "<br><br>" ;
         
         $cl_cursor = $cl->find() ;
         echo '<p>17. get next record</p>' ;
         // get next record
         $arr = $cl_cursor->getNext () ;
         var_dump ( $arr ) ;
         echo "<br><br>" ;
         
         echo '<p>18. update current record</p>' ;
         // update current record
         $arr = $cl_cursor->updateCurrent ( array ( '$set' => array ( 'd' => new SequoiaTimestamp ('2013-01-01-00.00.00.000000') ) ) ) ;
         var_dump ( $arr ) ;
         echo "<br><br>" ;
         
         
         echo '<p>19. get current record</p>' ;
         // get current record
         $cl_cursor = $cl->find() ;  // The cursor does not point a record
         $cl_cursor->getNext () ; // The cursor point the first record
         $arr = $cl_cursor->current () ; // get the current point record
         var_dump ( $arr ) ;
         echo "<br><br>" ;
         
         echo '<p>20. delete current record</p>' ;
         // get current record
         var_dump ( $cl_cursor->deleteCurrent() ) ;
         echo "<br><br>" ;
         
         echo '<p>21. remove all record</p>' ;
         // remove all record
         var_dump ( $cl->remove() ) ;
         echo "<br><br>" ;
         
         echo '<p>22. delete collection</p>' ;
         // delete collection
         var_dump ( $cl->drop() ) ;
         echo "<br><br>" ;
      }// if $cl
      echo '<p>23. delete collection space</p>' ;
      // delete collection
      var_dump ( $cs->drop() ) ;
      echo "<br><br>" ;
   }// if $cs
   
   echo '<p>24. reset snapshot</p>' ;
   // reset Snapshot
   var_dump ( $sdb->resetSnapshot() ) ;
   echo "<br><br>" ;
   
   echo '<p>25. get snapshot</p>' ;
   // reset Snapshot
   $sdb_cursor = $sdb->getSnapshot( SDB_SNAP_DATABASE ) ;
   if ( $sdb_cursor )
   {
      while ( $arr = $sdb_cursor->getNext() )
      {
         var_dump ( $arr ) ;
         echo "<br><br>" ;
      }
   }
   
   echo '<p>26. get list</p>' ;
   // reset Snapshot
   $sdb_cursor = $sdb->getList( SDB_LIST_STORAGEUNITS ) ;
   if ( $sdb_cursor )
   {
      while ( $arr = $sdb_cursor->getNext() )
      {
         var_dump ( $arr ) ;
         echo "<br><br>" ;
      }
   }
   
   echo '<p>27. disconnection</p>' ;
   // delete collection
   $sdb->close() ;
   echo "<br><br>" ;
}// if $sdb

?>