/*    Copyright 2012 SequoiaDB Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
/*    Copyright (C) 2011-2014 SequoiaDB Ltd.
 *    This program is free software: you can redistribute it and/or modify
 *    it under the term of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warrenty of
 *    MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program. If not, see <http://www.gnu.org/license/>.
 */


// This Header File is automatically generated, you MUST NOT modify this file anyway!
// On the contrary, you can modify the xml file "sequoiadb/misc/autogen/optlist.xml" if necessary!

#ifndef PMDOPTIONS_HPP_
#define PMDOPTIONS_HPP_

#include "pmdOptions.h"
#define PMD_COMMANDS_OPTIONS \
        ( PMD_COMMANDS_STRING (PMD_OPTION_HELP, ",h"), "help" ) \
        ( PMD_OPTION_VERSION, "Database version" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_DBPATH, ",d"), boost::program_options::value<string>(), "Database path" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_IDXPATH, ",i"), boost::program_options::value<string>(), "Index path" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_CONFPATH, ",c"), boost::program_options::value<string>(), "Configure file path" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_LOGPATH, ",l"), boost::program_options::value<string>(), "Log file path" ) \
        ( PMD_OPTION_DIAGLOGPATH, boost::program_options::value<string>(), "Diagnostic log file path" ) \
        ( PMD_OPTION_DIAGLOG_NUM, boost::program_options::value<int>(), "The max number of diagnostic log files, default:20, -1:unlimited" ) \
        ( PMD_OPTION_BKUPPATH, boost::program_options::value<string>(), "Backup path" ) \
        ( PMD_OPTION_MAXPOOL, boost::program_options::value<int>(), "The maximum number of pooled agent,defalut:0" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_SVCNAME, ",p"), boost::program_options::value<string>(), "Local service name or port" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_REPLNAME, ",r"), boost::program_options::value<string>(), "Replication service name or port, default: 'svcname'+1" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_SHARDNAME, ",a"), boost::program_options::value<string>(), "Sharding service name or port, default: 'svcname'+2" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_CATANAME, ",x"), boost::program_options::value<string>(), "Catalog service name or port, default: 'svcname'+3" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_RESTNAME, ",s"), boost::program_options::value<string>(), "REST service name or port, default: 'svcname'+4" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_DIAGLEVEL, ",v"), boost::program_options::value<int>(), "Diagnostic level,default:3,value range:[0-5]" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_ROLE, ",o"), boost::program_options::value<string>(), "Role of the node (data/coord/catalog/standalone)" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_CATALOG_ADDR, ",t"), boost::program_options::value<string>(), "Catalog addr (hostname1:servicename1,hostname2:servicename2,...)" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_LOGFILESZ, ",f"), boost::program_options::value<int>(), "Log file size ( in MB ),default:64,value range:[64,2048]" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_LOGFILENUM, ",n"), boost::program_options::value<int>(), "Number of log files,default:20, value range:[1,11800]" ) \
        ( PMD_COMMANDS_STRING (PMD_OPTION_TRANSACTIONON, ",e"), boost::program_options::value<string>(), "Turn on transaction, default:FALSE" ) \
        ( PMD_OPTION_NUMPRELOAD, boost::program_options::value<int>(), "The number of pre-loaders, default:0,value range:[0,100]" ) \
        ( PMD_OPTION_MAX_PREF_POOL, boost::program_options::value<int>(), "The maximum number of prefetchers, default:200, value range:[0,1000]" ) \
        ( PMD_OPTION_MAX_REPL_SYNC, boost::program_options::value<int>(), "The maximum number of repl-sync threads, default:10, value range:[0, 200], 0:disable concurrent repl-sync" ) \
        ( PMD_OPTION_LOGBUFFSIZE, boost::program_options::value<int>(), "The number of pages ( in 64KB ) for replica log memory ( default:1024, value range:[512,1024000] ), the size should be smaller than log space" ) \
        ( PMD_OPTION_DMS_TMPBLKPATH, boost::program_options::value<string>(), "The path of the temp files" ) \
        ( PMD_OPTION_SORTBUF_SIZE, boost::program_options::value<int>(), "Size of the sorting buf(MB), default:256, min value:128" ) \
        ( PMD_OPTION_HJ_BUFSZ, boost::program_options::value<int>(), "Size of the hash join buf(MB), default:128, min value:64" ) \
        ( PMD_OPTION_SYNC_STRATEGY, boost::program_options::value<string>(), "The control strategy of data sync in ReplGroup, value enumeration: none,keepnormal,keepall, default:keepnormal." ) \
        ( PMD_OPTION_PREFINST, boost::program_options::value<string>(), "Prefered instance for read request, default:A, value enum:M,S,A,1-7" ) \
        ( PMD_OPTION_NUMPAGECLEANERS, boost::program_options::value<int>(), "Number of page cleaners, default 1, value range:[0, 50]" ) \
        ( PMD_OPTION_PAGECLEANINTERVAL, boost::program_options::value<int>(), "The minimal interval between two cleanup actions for each collection space (in ms, default 10000, min 1000 )" ) \
        ( PMD_OPTION_LOBPATH, boost::program_options::value<string>(), "Large object file path" ) \
        ( PMD_OPTION_DIRECT_IO_IN_LOB, boost::program_options::value<string>(), "Open direct io in large object" ) \
        ( PMD_OPTION_SPARSE_FILE, boost::program_options::value<string>(), "extend the file as a sparse file" ) \

#define PMD_HIDDEN_COMMANDS_OPTIONS \
        ( PMD_OPTION_WWWPATH, boost::program_options::value<string>(), "Web service root path" ) \
        ( PMD_OPTION_OMNAME, boost::program_options::value<string>(), "OM service name or port, default: 'svcname'+5" ) \
        ( PMD_OPTION_MAX_SUB_QUERY, boost::program_options::value<int>(), "The maximum number of sub-query for each SQL request, default:10, min value is 0, max value can't more than param 'maxprefpool'" ) \
        ( PMD_OPTION_REPL_BUCKET_SIZE, boost::program_options::value<int>(), "Repl bucket size ( must be the power of 2 ), default is 32, value range:[1,4096]" ) \
        ( PMD_OPTION_MEMDEBUG, boost::program_options::value<string>(), "Enable memory debug, default:FALSE" ) \
        ( PMD_OPTION_MEMDEBUGSIZE, boost::program_options::value<int>(), "Memory debug segment size,default:0, if not zero, the value range:[256,4194304]" ) \
        ( PMD_OPTION_CATALIST, boost::program_options::value<string>(), "Catalog node list(json)" ) \
        ( PMD_OPTION_DPSLOCAL, boost::program_options::value<string>(), "Log the operation from local port, default:FALSE" ) \
        ( PMD_OPTION_TRACEON, boost::program_options::value<string>(), "Turn on trace when starting, default:FALSE" ) \
        ( PMD_OPTION_TRACEBUFSZ, boost::program_options::value<int>(), "Trace buffer size, default:268435456, value range:[524288,1073741824]" ) \
        ( PMD_OPTION_SHARINGBRK, boost::program_options::value<int>(), "The timeout period for heartbeat in each replica group ( in ms ), default:5000, value range:[5000,300000] " ) \
        ( PMD_OPTION_INDEX_SCAN_STEP, boost::program_options::value<int>(), "Index scan step, default is 100, range:[1, 10000]" ) \
        ( PMD_OPTION_START_SHIFT_TIME, boost::program_options::value<int>(), "Nodes starting shift time(sec), default:600, value range:[0,7200]" ) \

#endif /* PMDOPTIONS_HPP_ */