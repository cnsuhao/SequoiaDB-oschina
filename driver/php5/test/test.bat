test.exe "{hello :  \"world\"}"
test.exe "{date1 : { $timestamp: 2012-07-21-00.00.00.1 } }"
test.exe "{date2 : { $date: 2012-07-21 } }"
test.exe "{ $and : { $or : { $not : { d : { $exists : 0 } } , e : { $exists : 1 } } } }"
pause