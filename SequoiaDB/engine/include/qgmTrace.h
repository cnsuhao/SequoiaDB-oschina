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

/* This list file is automatically generated,you shoud NOT modify this file anyway! test comment*/
#ifndef qgmTRACE_H__
#define qgmTRACE_H__
#define SDB__QGMPLDELETE__EXEC                             0x800000000000149L
#define SDB__QGMPARAMTABLE_ADDCONST                        0x80000000000014aL
#define SDB__QGMPARAMTABLE_ADDCONST2                       0x80000000000014bL
#define SDB__QGMPARATABLE_ADDVAR                           0x80000000000014cL
#define SDB__QGMPARAMTABLE_SETVAR                          0x80000000000014dL
#define SDB__QGMPLSORT__EXEC                               0x80000000000014eL
#define SDB__QGMPLSORT__FETCHNEXT                          0x80000000000014fL
#define SDB__QGMOPSTREAM_FIND                              0x800000000000150L
#define SDB__QGMOPRUNIT_ADDOPFIELD                         0x800000000000151L
#define SDB__QGMOPTITREENODE_ADDCHILD                      0x800000000000152L
#define SDB__QGMOPTITREENODE__ONPUSHOPRUNIT                0x800000000000153L
#define SDB__QGMOPTITREENODE_PUSHOPRUNIT                   0x800000000000154L
#define SDB__QGMOPTITREENODE_RMOPRUNIT                     0x800000000000155L
#define SDB__QGMOPTITREENODE_UPCHANGE                      0x800000000000156L
#define SDB__QGMOPTITREENODE_OUTPUTSORT                    0x800000000000157L
#define SDB__QGMOPTITREENODE_EXTEND                        0x800000000000158L
#define SDB__QGMOPTTREE__PREPARE                           0x800000000000159L
#define SDB__QGMOPTTREE_INSERTBETWEEN                      0x80000000000015aL
#define SDB__QGMPLHASHJOIN_INIT                            0x80000000000015bL
#define SDB__QGMPLHASHJOIN__EXEC                           0x80000000000015cL
#define SDB__QGMPLHASHJOIN__FETCHNEXT                      0x80000000000015dL
#define SDB__QGMPLHASHJOIN__BUILDHASNTBL                   0x80000000000015eL
#define SDB__QGMOPTIAGGREGATION_INIT                       0x80000000000015fL
#define SDB__QGMOPTIAGGREGATION_DONE                       0x800000000000160L
#define SDB__QGMOPTIAGGREGATION__UPDATE2UNIT               0x800000000000161L
#define SDB__QGMOPTIAGGREGATION__ADDFIELDS                 0x800000000000162L
#define SDB__QGMOPTIAGGREGATION__GETFIELDALIAS             0x800000000000163L
#define SDB__QGMOPTIAGGREGATION__PUSHOPRUNIT               0x800000000000164L
#define SDB__QGMOPTIAGGREGATION_OURPUTSORT                 0x800000000000165L
#define SDB__QGMOPTIAGGREGATION_PARSE                      0x800000000000166L
#define SDB__QGMPLSPLITBY__FETCHNEXT                       0x800000000000167L
#define SDB__QGMFETCHOUT_ELEMENT                           0x800000000000168L
#define SDB__QGMFETCHOUT_ELEMENTS                          0x800000000000169L
#define SDB__QGMBUILDER_BUILDORDERBY                       0x80000000000016aL
#define SDB__QGMBUILDER_BUILD1                             0x80000000000016bL
#define SDB__QGMBUILDER_BUILD2                             0x80000000000016cL
#define SDB__QGMBUILDER__BUILDPHYNODE                      0x80000000000016dL
#define SDB__QGMBUILDER__ADDPHYCOMMAND                     0x80000000000016eL
#define SDB__QGMBUILDER__ADDPHYAGGR                        0x80000000000016fL
#define SDB__QGMBUILDER__CRTPHYSORT                        0x800000000000170L
#define SDB__QGMBUILDER__ADDPHYSCAN                        0x800000000000171L
#define SDB__QGMBUILDER__ADDMTHMATHERSCAN                  0x800000000000172L
#define SDB__QGMBUILDER__CRTPHYJOIN                        0x800000000000173L
#define SDB__QGMBUILDER__CRTPHYFILTER                      0x800000000000174L
#define SDB__QGMBUILDER__CRTMTHMATHERFILTER                0x800000000000175L
#define SDB__QGMBUILDER__BUILD1                            0x800000000000176L
#define SDB__QGMBUILDER__BUILDUPDATE                       0x800000000000177L
#define SDB__QGMBUILDER__ADDSET                            0x800000000000178L
#define SDB__QGMBUILDER__BUILDDELETE                       0x800000000000179L
#define SDB__QGMBUILDER__BUILDDROPCL                       0x80000000000017aL
#define SDB__QGMBUILDER__BUILDDROPINX                      0x80000000000017bL
#define SDB__QGMBUILDER__BUILDCRTINX                       0x80000000000017cL
#define SDB__QGMBUILDER__BUILDINXCOLUMNS                   0x80000000000017dL
#define SDB__QGMBUILDER__BUILDCRTCL                        0x80000000000017eL
#define SDB__QGMBUILDER__BUILDCRTCS                        0x80000000000017fL
#define SDB__QGMBUILDER__BUILDDROPCS                       0x800000000000180L
#define SDB__QGMBUILDER__BUILDSELECT                       0x800000000000181L
#define SDB__QGMBUILDER__BUILDINSERT                       0x800000000000182L
#define SDB__QGMBUILDER__ADDSELECTOR                       0x800000000000183L
#define SDB__QGMBUILDER__ADDFROM                           0x800000000000184L
#define SDB__QGMBUILDER__BUILDJOIN                         0x800000000000185L
#define SDB__QGMBUILDER__BUILDINCONDITION                  0x800000000000186L
#define SDB__QGMBUILDER__BUILDCONDITION                    0x800000000000187L
#define SDB__QGMBUILDER__ADDSPLITBY                        0x800000000000188L
#define SDB__QGMBUILDER__ADDGROUPBY                        0x800000000000189L
#define SDB__QGMBUILDER__ADDORDERBY                        0x80000000000018aL
#define SDB__QGBUILDER__ADDLIMIT                           0x80000000000018bL
#define SDB__QGMBUILDER__ADDSKIP                           0x80000000000018cL
#define SDB__QGMBUILDER__ADDCOLUMNS                        0x80000000000018dL
#define SDB__QGMBUILDER__ADDVALUES                         0x80000000000018eL
#define SDB__QGMBUILDER__ADDHINT                           0x80000000000018fL
#define SDB__QGMOPTINLJOIN__MAKECONDVAR                    0x800000000000190L
#define SDB__QGMOPTINLJOIN_MAKECONDITION                   0x800000000000191L
#define SDB__QGMOPTINLJOIN_INIT                            0x800000000000192L
#define SDB__QGMOPTINLJOIN__CRTJOINUNIT                    0x800000000000193L
#define SDB__QGMOPTINLJOIN_OUTPUTSTREAM                    0x800000000000194L
#define SDB__QGMOPTINLJOIN__PUSHOPRUNIT                    0x800000000000195L
#define SDB__QGMOPTINLJOIN__UPDATECHANGE                   0x800000000000196L
#define SDB__QGMOPTINLJOIN_HANDLEHINTS                     0x800000000000197L
#define SDB__QGMOPTINLJOIN__VALIDATE                       0x800000000000198L
#define SDB__QGMOPTISELECT_INIT                            0x800000000000199L
#define SDB__QGMOPTISELECT_DONE                            0x80000000000019aL
#define SDB__QGMOPTISELECT_OUTPUTSTREAM                    0x80000000000019bL
#define SDB__QGMOPTISELECT__PUSHOPRUNIT                    0x80000000000019cL
#define SDB__QGMOPTISELECT__RMOPRUNIT                      0x80000000000019dL
#define SDB__QGMOPTISELECT__EXTEND                         0x80000000000019eL
#define SDB__QGMOPTISELECT__VALIDATEANDCRTPLAN             0x80000000000019fL
#define SDB__QGMOPTISELECT__PARAMEXISTINSELECOTR           0x8000000000001a0L
#define SDB__QGMHASHTBL_PUSH                               0x8000000000001a1L
#define SDB__QGMHASHTBL_FIND                               0x8000000000001a2L
#define SDB__QGMHASHTBL_GETMORE                            0x8000000000001a3L
#define SDB__QGMSELECTOR_SELECT                            0x8000000000001a4L
#define SDB__QGMSELECTOR_SELECT2                           0x8000000000001a5L
#define SDB__QGMPLSCAN__EXEC                               0x8000000000001a6L
#define SDB__QGMPLSCAN__EXECONDATA                         0x8000000000001a7L
#define SDB__QGMPLSCAN__EXECONCOORD                        0x8000000000001a8L
#define SDB__QGMPLSCAN__FETCHNEXT                          0x8000000000001a9L
#define SDB__QGMPLSCAN__FETCH                              0x8000000000001aaL
#define SDB__QGMPLNLJOIN__INIT                             0x8000000000001abL
#define SDB__QGMPLNLJOIN__EXEC                             0x8000000000001acL
#define SDB__QGMPLNLJOIN__FETCHNEXT                        0x8000000000001adL
#define SDB__QGMPLNLJOIN__MODIFYINNERCONDITION             0x8000000000001aeL
#define SDB__QGMEXTENDPLAN_EXTEND                          0x8000000000001afL
#define SDB__QGMEXTENDPLAN_INSERTPLAN                      0x8000000000001b0L
#define SDB__QGMPLFILTER__FETCHNEXT                        0x8000000000001b1L
#define SDB__QGMUTILFIRSTDOT                               0x8000000000001b2L
#define SDB__QGMFINDFIELDFROMFUNC                          0x8000000000001b3L
#define SDB__QGMISFROMONE                                  0x8000000000001b4L
#define SDB__QGMISSAMEFROM                                 0x8000000000001b5L
#define SDB__QGMMERGE                                      0x8000000000001b6L
#define SDB__QGMREPLACEFIELDRELE                           0x8000000000001b7L
#define SDB__QGMREPLACEATTRRELE                            0x8000000000001b8L
#define SDB__QGMREPLACEATTRRELE2                           0x8000000000001b9L
#define SDB__QGMREPLACEATTRRELE3                           0x8000000000001baL
#define SDB__QDMDOWNFIELDSBYFIELDALIAS                     0x8000000000001bbL
#define SDB__QGMDOWNATTRSBYFIELDALIAS                      0x8000000000001bcL
#define SDB__QGMDOWNATTRSBYFIELDALIAS2                     0x8000000000001bdL
#define SDB__QGMDOWNAATTRBYFIELDALIAS                      0x8000000000001beL
#define SDB__QGMDOWNAGGRSBYFIELDALIAS                      0x8000000000001bfL
#define SDB__QGMUPFIELDSBYFIELDALIAS                       0x8000000000001c0L
#define SDB__QGMUPATTRSBYFIELDALIAS                        0x8000000000001c1L
#define SDB__QGMUPATTRSBYFIELDALIAS2                       0x8000000000001c2L
#define SDB__QGMUPAATTRBYFIELDALIAS                        0x8000000000001c3L
#define SDB__QGMUPATTRSBYFIELDALIAS3                       0x8000000000001c4L
#define SDB__QGMMATCHER_MATCH                              0x8000000000001c5L
#define SDB__QGMMATCHER__MATCH                             0x8000000000001c6L
#define SDB__QGMCONDITIONNODEHELPER_MERGE                  0x8000000000001c7L
#define SDB__QGMCONDITIONNODEHELPER_SEPARATE               0x8000000000001c8L
#define SDB__QGMCONDITIONNODEHELPER_SEPARATE2              0x8000000000001c9L
#define SDB__QGMCONDITIONNODEHELPER__CRTBSON               0x8000000000001caL
#define SDB__QGMCONDITIONNODEHELPER__TOBSON                0x8000000000001cbL
#define SDB__QGMCONDITIONNODEHELPER__GETALLATTR            0x8000000000001ccL
#define SDB__QGMPLCOMMAND__EXEC                            0x8000000000001cdL
#define SDB__QGMPLCOMMAND_EXECONCOORD                      0x8000000000001ceL
#define SDB__QGMPLCOMMAND__EXECONDATA                      0x8000000000001cfL
#define SDB__QGMPLCOMMAND__FETCHNEXT                       0x8000000000001d0L
#define SDB__QGMPLMTHMATCHERSCAN__EXEC                     0x8000000000001d1L
#define SDB__QGMPLMTHMATCHERFILTER__FETCHNEXT              0x8000000000001d2L
#define SDB__QGMPTRTABLE_GETFIELD                          0x8000000000001d3L
#define SDB__QGMPTRTABLE_GETOWNFIELD                       0x8000000000001d4L
#define SDB__QGMPTRTABLE_GETATTR                           0x8000000000001d5L
#define SDB__QGMPLUPDATE__EXEC                             0x8000000000001d6L
#define SDB__QGMOPTISORT_INIT                              0x8000000000001d7L
#define SDB__QGMOPTISORT__PUSHOPRUNIT                      0x8000000000001d8L
#define SDB__QGMOPTISORT__RMOPRUNIT                        0x8000000000001d9L
#define SDB__QGMOPTISORT_APPEND                            0x8000000000001daL
#define SDB__QGMPLAN_EXECUTE                               0x8000000000001dbL
#define SDB__QGMPLAN_FETCHNEXT                             0x8000000000001dcL
#define SDB__QGMEXTENDSELECTPLAN__EXTEND                   0x8000000000001ddL
#define SDB__QGMPLINSERT__MERGEOBJ                         0x8000000000001deL
#define SDB__QGMPLINSERT__NEXTRECORD                       0x8000000000001dfL
#define SDB__QGMPLINSERT__EXEC                             0x8000000000001e0L
#endif
