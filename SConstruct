# -*- mode: python; -*-
# build file for SequoiaDB
# this requires scons
# you can get from http://www.scons.org
# then just type scons

# some common tasks
#   build 64-bit mac and pushing to s3
#      scons --64 s3dist

# This file, SConstruct, configures the build environment, and then delegates to
# several, subordinate SConscript files, which describe specific build rules.

EnsureSConsVersion( 1, 1, 0 )

import platform
import os
import sys
import imp
import types
import re
import shutil
import urllib
import urllib2
import stat
from os.path import join, dirname, abspath
import libdeps
root_dir = dirname(File('SConstruct').rfile().abspath)
db_dir = join(root_dir,'SequoiaDB')
engine_dir = join(db_dir,'engine')
thirdparty_dir = join(root_dir, 'thirdparty')
boost_dir = join(thirdparty_dir, 'boost')
boost_lib_dir = join(boost_dir, 'lib')
parser_dir = join(thirdparty_dir, 'parser' )
sm_dir = join(parser_dir, 'sm')
js_dir = join(sm_dir, 'js')
pcre_dir = join(engine_dir,'pcre')
ssh2_dir = join(engine_dir,'ssh2')
crypto_dir = join(thirdparty_dir, 'crypto')
ssl_dir = join(crypto_dir, 'openssl-1.0.1c')
gtest_dir = join(engine_dir,'gtest')
ncursesinclude_dir = join(engine_dir, 'ncurses/include')
driver_dir = join(db_dir,'driver')
java_dir = join(root_dir,'java')
# --- options ----

options = {}

options_topass = {}

def GuessOS():
   id = platform.system()
   if id == 'Linux':
      return 'linux'
   elif id == 'Windows' or id == 'Microsoft':
      return 'win32'
   else:
      return None


def GuessArch():
   id = platform.machine()
   id = id.lower()
   if (not id) or (not re.match('(x|i[3-6])86$', id) is None):
      return 'ia32'
   elif id == 'i86pc':
      return 'ia32'
   elif id == 'x86_64':
      return 'ia64'
   elif id == 'amd64':
      return 'ia64'
   elif id == 'ppc64':
      return 'ppc64'
   else:
      return None

# guess the operating system and architecture
guess_os = GuessOS()
guess_arch = GuessArch()

# helper function, add options
# name: name of the parameter
# nargs: number of args for the parameter
# contibutesToVariantDir: whether the param is part of variant dir
def add_option( name, help , nargs , contibutesToVariantDir , dest=None ):

    if dest is None:
        dest = name

    AddOption( "--" + name ,
               dest=dest,
               type="string",
               nargs=nargs,
               action="store",
               help=help )

    options[name] = { "help" : help ,
                      "nargs" : nargs ,
                      "contibutesToVariantDir" : contibutesToVariantDir ,
                      "dest" : dest }

def get_option( name ):
    return GetOption( name )

def _has_option( name ):
    x = get_option( name )
    if x is None:
        return False

    if x == False:
        return False

    if x == "":
        return False

    return True

def has_option( name ):
    x = _has_option(name)

    if name not in options_topass:
        # if someone already set this, don't overwrite
        options_topass[name] = x

    return x


def get_variant_dir():

    a = []

    for name in options:
        o = options[name]
        if not has_option( o["dest"] ):
            continue
        # let's skip the param if it's not part of variant dir
        if not o["contibutesToVariantDir"]:
            continue

        if o["nargs"] == 0:
            a.append( name )
        else:
            x = get_option( name )
            x = re.sub( "[,\\\\/]" , "_" , x )
            a.append( name + "_" + x )

    s = "#build/${PYSYSPLATFORM}/"

    if len(a) > 0:
        a.sort()
        s += "/".join( a ) + "/"
    else:
        s += "normal/"
    return s

# build options
add_option( "all", "build engine/tools/testcases/shell/client/fmp", 0, False)
add_option( "engine", "build engine", 0, False)
add_option( "tool", "build tools", 0, False)
add_option( "testcase", "build testcases", 0, False)
add_option( "shell", "build shell", 0, False)
add_option( "client", "build C/C++ clients", 0, False)
add_option( "fmp", "build fmp", 0, False)

# language could be en or cn
add_option( "language" , "description language" , 1 , False )

# linking options
add_option( "release" , "release build" , 0 , True )

# dev options
add_option( "dd", "debug build no optimization" , 0 , True , "debugBuild" )
add_option( "noscreenout", "do not send anything to screen", 0, True )

# don't run configure if user calls --help
if GetOption('help'):
    Return()

# --- environment setup ---
variantDir = get_variant_dir()
clientCppVariantDir = variantDir + "clientcpp"
clientCVariantDir = variantDir + "clientc"
shellVariantDir = variantDir + "shell"
toolVariantDir = variantDir + "tool"
fmpVariantDir = variantDir + "fmp"
driverDir = variantDir + "driver"

def printLocalInfo():
   import sys, SCons
   print( "scons version: " + SCons.__version__ )
   print( "python version: " + " ".join( [ `i` for i in sys.version_info ] ) )

printLocalInfo()

boostLibs = [ "thread" , "filesystem", "program_options", "system" ]

nix = False
linux = False
linux64  = False
windows = False

release = True
debugBuild = False

release = has_option( "release" )

# get whether we are using debug build
debugBuild = has_option( "debugBuild" )

# if neither release/debugBuild specified, by default using release
# if both release/debugBuild specified, by defaul use debugBuild
if not release and not debugBuild:
   release = True
   debugBuild = False
elif release and debugBuild:
   release = False
   debugBuild = True

env = Environment( BUILD_DIR=variantDir,
                   tools=["default", "gch", "mergelib" ],
                   PYSYSPLATFORM=os.sys.platform,
                   )

libdeps.setup_environment( env )

if env['PYSYSPLATFORM'] == 'linux3':
   env['PYSYSPLATFORM'] = 'linux2'

if os.sys.platform == 'win32':
   env['OS_FAMILY'] = 'win'
else:
   env['OS_FAMILY'] = 'posix'

if env['PYSYSPLATFORM'] == 'linux2':
   env['LINK_LIBGROUP_START'] = '-Wl,--start-group'
   env['LINK_LIBGROUP_END'] = '-Wl,--end-group'
   env['RELOBJ_LIBDEPS_START'] = '--whole-archive'
   env['RELOBJ_LIBDEPS_END'] = '--no-whole-archive'
   env['RELOBJ_LIBDEPS_ITEM'] = ''

env["LIBPATH"] = []

if has_option( "noscreenout" ):
    env.Append( CPPDEFINES=[ "_NOSCREENOUT" ] )

hasEngine = has_option( "engine" )
hasClient = has_option( "client" )
hasTestcase = has_option( "testcase" )
hasTool = has_option( "tool" )
hasShell = has_option( "shell" )
hasFmp = has_option("fmp")
hasAll = has_option( "all" )

# if everything are set, let's set everything to true
if hasAll:
   hasEngine = True
   hasClient = True
   hasTestcase = True
   hasTool = True
   hasShell = True
   hasFmp = True
# if nothing specified, let's use engine+client+shell by default
elif not ( hasEngine or hasClient or hasTestcase or hasTool or hasShell or hasFmp ):
   hasEngine = True
   hasClient = True
   hasShell = True

boostCompiler = ""
boostVersion = ""

usesm = True

extraLibPlaces = []

env['EXTRACPPPATH'] = []
env['EXTRALIBPATH'] = []

class InstallSetup:
    binaries = False
    clientSrc = False
    headers = False
    bannerFiles = tuple()
    headerRoot = "include"

    def __init__(self):
        self.default()

    def default(self):
        self.binaries = True
        self.libraries = False
        self.clientSrc = False
        self.headers = False
        self.bannerFiles = tuple()
        self.headerRoot = "include"
        self.clientTestsDir = None

    def justClient(self):
        self.binaries = False
        self.libraries = False
        self.clientSrc = True
        self.headers = True
        self.bannerFiles = [ "#distsrc/client/LICENSE.txt",
                             "#distsrc/client/SConstruct" ]
        self.headerRoot = ""

installSetup = InstallSetup()

# ---- other build setup -----

platform = os.sys.platform
if "uname" in dir(os):
    processor = os.uname()[4]
else:
    processor = "i386"

env['PROCESSOR_ARCHITECTURE'] = processor

DEFAULT_INSTALL_DIR = "/opt/sequoiadb"
installDir = DEFAULT_INSTALL_DIR
nixLibPrefix = "lib"

def findVersion( root , choices ):
    if not isinstance(root, list):
        root = [root]
    for r in root:
        for c in choices:
            if ( os.path.exists( r + c ) ):
                return r + c
    raise RuntimeError("can't find a version of [" + repr(root) + "] choices: " + repr(choices))

# add database include, boost include here
hdfsJniPath = ""
hdfsJniMdPath = ""
if guess_os == "linux":
    if guess_arch == "ia32":
        hdfsJniPath = join(java_dir,"jdk_linux32/include")
        hdfsJniMdPath = join(java_dir,"jdk_linux32/include/linux")
    elif guess_arch == "ia64":
        hdfsJniPath = join(java_dir,"jdk_linux64/include")
        hdfsJniMdPath = join(java_dir,"jdk_linux64/include/linux")
    elif guess_arch == "ppc64":
        hdfsJniPath = join(java_dir,"jdk_ppclinux64/include")
        hdfsJniMdPath = join(java_dir,"jdk_ppclinux64/include/linux")
elif guess_os == "win32":
    if guess_arch == "ia32":
        hdfsJniPath = join(java_dir,"jdk_win32/include")
        hdfsJniMdPath = join(java_dir,"jdk_win32/include/win32")
    elif guess_arch == "ia64":
        hdfsJniPath = join(java_dir,"jdk_win64/include")
        hdfsJniMdPath = join(java_dir,"jdk_win64/include/win32")

env.Append( CPPPATH=[join(engine_dir,'include'),join(engine_dir,'client'),join(ssl_dir,'include'),join(gtest_dir,'include'),pcre_dir, boost_dir, ssh2_dir, hdfsJniPath, hdfsJniMdPath] )

env.Append( CPPDEFINES=["__STDC_LIMIT_MACROS", "HAVE_CONFIG_H"] )
env.Append( CPPDEFINES=[ "SDB_DLL_BUILD" ] )
# specify dependent libraries for javascript engine and boost
if guess_os == "linux":
    linux = True
    platform = "linux"

    # -lm
    env.Append( LIBS=['m'] )
    # -ldl
    env.Append( LIBS=['dl'] )
    # -lpthread
    env.Append( LIBS=["pthread"] )
    # GNU
    env.Append( CPPDEFINES=[ "_GNU_SOURCE" ] )
    # 64 bit linux
    if guess_arch == "ia64":
        linux64 = True
        nixLibPrefix = "lib64"
        boost_lib_dir = join(boost_lib_dir,'linux64')
        env.Append( EXTRALIBPATH="/lib64" )
        # use project-related boost library
        env.Append( EXTRALIBPATH=boost_lib_dir )
        # use project-related ssl library
        env.Append( EXTRALIBPATH=join(ssl_dir,'lib/linux64') )
        # use project-related spidermonkey library
        if usesm:
            if debugBuild:
                smlib_dir = join(js_dir,'lib/debug/linux64/lib')
                env.Append( CPPPATH=join(js_dir,'lib/debug/linux64/include') )
                env.Append( EXTRALIBPATH=[smlib_dir] )
            else:
                smlib_dir = join(js_dir,'lib/release/linux64/lib')
                env.Append( CPPPATH=join(js_dir,'lib/release/linux64/include') )
                env.Append( EXTRALIBPATH=[smlib_dir] )
        ssllib_dir = join(ssl_dir,'lib/linux64')
    # in case for 32 bit linux or compiling 32 bit in 64 env
    elif guess_arch == "ia32":
        linux64 = False
        nixLibPrefix = "lib"
        boost_lib_dir = join(boost_lib_dir,'linux32')
        env.Append( EXTRALIBPATH="/lib" )
        # we want 32 bit boost library
        env.Append( EXTRALIBPATH=boost_lib_dir )
        # use project-related ssl library
        env.Append( EXTRALIBPATH=join(ssl_dir,'lib/linux32') )
        # and 32 bit spidermonkey library
        if usesm:
            if debugBuild:
                smlib_dir = join(js_dir,'lib/debug/linux32/lib')
                env.Append( CPPPATH=join(js_dir,'lib/debug/linux32/include') )
                env.Append( EXTRALIBPATH=[smlib_dir] )
            else:
                smlib_dir = join(js_dir,'lib/release/linux32/lib')
                env.Append( CPPPATH=join(js_dir,'lib/release/linux32/include') )
                env.Append( EXTRALIBPATH=[smlib_dir] )
                # if we are in 64 bit box but want to build 32 bit release
        ssllib_dir = join(ssl_dir,'lib/linux32')
    # power pc linux
    elif guess_arch == "ppc64":
        linux64 = True
        nixLibPrefix = "lib64"
        boost_lib_dir = join(boost_lib_dir,'ppclinux64')
        # use big endian
        env.Append( CPPDEFINES=[ "SDB_BIG_ENDIAN" ] )
        #env.Append( EXTRALIBPATH="/usr/lib64" )
        env.Append( EXTRALIBPATH="/lib64" )
        # use project-related boost library
        env.Append( EXTRALIBPATH=boost_lib_dir )
        # use project-related ssl library
        env.Append( EXTRALIBPATH=join(ssl_dir,'lib/ppclinux64') )
        # use project-related spidermonkey library
        if usesm:
            if debugBuild:
                smlib_dir = join(js_dir,'lib/debug/ppclinux64/lib')
                env.Append( CPPPATH=join(js_dir,'lib/debug/ppclinux64/include') )
                env.Append( EXTRALIBPATH=[smlib_dir] )
            else:
                smlib_dir = join(js_dir,'lib/release/ppclinux64/lib')
                env.Append( CPPPATH=join(js_dir,'lib/release/ppclinux64/include') )
                env.Append( EXTRALIBPATH=[smlib_dir] )
        ssllib_dir = join(ssl_dir,'lib/ppclinux64')

    # spider monkey
    if usesm:
        smlib_file = join(smlib_dir, 'libmozjs185.so')
        env.Append( CPPDEFINES=[ "XP_UNIX" ] )
        env.Append( LIBS=['js_static'] )
    # SSL
    env.Append( LIBS=['crypto'] )
    ssllib_file = join(ssllib_dir, 'libcrypto.a')
    ssllib_file1 = join(ssllib_dir, 'libcrypto.a')
    nix = True

elif "win32" == guess_os:
    # when building windows
    windows = True
    # check VC compiler
    for pathdir in env['ENV']['PATH'].split(os.pathsep):
        if os.path.exists(os.path.join(pathdir, 'cl.exe')):
            print( "found visual studio at " + pathdir )
            break
        else:
            #use current environment
            env['ENV'] = dict(os.environ)

    # if we are 64 bit
    if guess_arch == "ia64":
        boost_lib_dir = join(boost_lib_dir,'win64')
        # use 64 bit boost library
        env.Append( EXTRALIBPATH=boost_lib_dir )
        # use project-related ssl library
        env.Append( EXTRALIBPATH=join(ssl_dir,'lib/win64') )
        # use 64 bit spidermonkey
        if usesm:
            if debugBuild:
                smlib_dir = join(js_dir,'lib/debug/win64/lib')
                env.Append( CPPPATH=join(js_dir,'lib/debug/win64/include') )
                env.Append( EXTRALIBPATH=[smlib_dir] )
            else:
                smlib_dir = join(js_dir,'lib/release/win64/lib')
                env.Append( CPPPATH=join(js_dir,'lib/release/win64/include') )
                env.Append( EXTRALIBPATH=[smlib_dir] )
        ssllib_dir = join(ssl_dir,'lib/win64')
    else:
        boost_lib_dir = join(boost_lib_dir,'win32')
        # we are 32 bit
        env.Append( EXTRALIBPATH=boost_lib_dir )
        # use project-related ssl library
        env.Append( EXTRALIBPATH=join(ssl_dir,'lib/win32') )
        if usesm:
            if debugBuild:
                smlib_dir = join(js_dir,'lib/debug/win32/lib')
                env.Append( CPPPATH=join(js_dir,'lib/debug/win32/include') )
                env.Append( EXTRALIBPATH=[smlib_dir] )
            else:
                smlib_dir = join(js_dir,'lib/release/win32/lib')
                env.Append( CPPPATH=join(js_dir,'lib/release/win32/include') )
                env.Append( EXTRALIBPATH=[smlib_dir] )
        ssllib_dir = join(ssl_dir,'lib/win32')
    if usesm:
        smlib_file = join(smlib_dir, 'mozjs185-1.0.dll')
        env.Append( CPPDEFINES=[ "XP_WIN" ] )
        env.Append( LIBS=['mozjs185-1.0'] )
        env.Append( CPPDEFINES=["JS_HAVE_STDINT_H"] )
    # SSL
    env.Append( LIBS=['libeay32'] )
    ssllib_file = join(ssllib_dir, 'libeay32.dll')
    ssllib_file1 = join(ssllib_dir, 'ssleay32.dll')
    # UNICODE
    env.Append( CPPDEFINES=[ "_UNICODE" ] )
    env.Append( CPPDEFINES=[ "UNICODE" ] )
    # find windows SDK
    winSDKHome = findVersion( [ "C:/Program Files/Microsoft SDKs/Windows/", "C:/Program Files (x86)/Microsoft SDKs/Windows/" ] ,
                              [ "v7.1", "v7.0A", "v7.0", "v6.1", "v6.0a", "v6.0" ] )
    print( "Windows SDK Root '" + winSDKHome + "'" )

    env.Append( EXTRACPPPATH=[ winSDKHome + "/Include" ] )

    env.Append( CPPFLAGS=" /EHsc /W3 " )

    env.Append( CPPFLAGS=" /wd4355 /wd4800 /wd4267 /wd4244 /wd4200 " )

    env.Append( CPPDEFINES=["_CONSOLE","_CRT_SECURE_NO_WARNINGS","PSAPI_VERSION=1","_CRT_RAND_S" ] )

    if release:
        env.Append( CPPDEFINES=[ "NDEBUG" ] )
        env.Append( CPPFLAGS= " /O2 /Gy " )
        env.Append( CPPFLAGS= " /MT /Zi /errorReport:none " )
        env.Append( CPPFLAGS= " /GL " )
        env.Append( LINKFLAGS=" /LTCG " )
        env.Append( LINKFLAGS=" /DEBUG " )
    else:
        env.Append( CPPFLAGS=" /RTC1 /MDd /Z7 /errorReport:none " )

        if debugBuild:
            env.Append( LINKFLAGS=" /debug " )
            env.Append( CPPFLAGS=" /Od " )
            env.Append( CPPDEFINES=[ "_DEBUG" ] )

    if guess_arch == "ia64":
        env.Append( EXTRALIBPATH=[ winSDKHome + "/Lib/x64" ] )
    else:
        env.Append( EXTRALIBPATH=[ winSDKHome + "/Lib" ] )

    if release:
        env.Append( LINKFLAGS=" /NODEFAULTLIB:MSVCPRT  " )
    else:
        env.Append( LINKFLAGS=" /NODEFAULTLIB:MSVCPRT  /NODEFAULTLIB:MSVCRT  " )

    winLibString = "ws2_32.lib kernel32.lib advapi32.lib Psapi.lib"

    winLibString += " user32.lib gdi32.lib winspool.lib comdlg32.lib  shell32.lib ole32.lib oleaut32.lib "
    winLibString += " odbc32.lib odbccp32.lib uuid.lib dbghelp.lib "

    env.Append( LIBS=Split(winLibString) )
else:
    print( "No special config for [" + os.sys.platform + "] which probably means it won't work" )

env['STATIC_AND_SHARED_OBJECTS_ARE_THE_SAME'] = 1
if nix:
    env.Append( CPPFLAGS="-fPIC -fno-strict-aliasing -ggdb -pthread -Wno-write-strings -Wall -Wsign-compare -Wno-unknown-pragmas -Winvalid-pch -Wno-address" )
    if linux:
        env.Append( CPPFLAGS=" -pipe " )
        env.Append( CPPFLAGS=" -fno-builtin-memcmp " )

    env.Append( CPPDEFINES="_FILE_OFFSET_BITS=64" )
    env.Append( CXXFLAGS=" -Wnon-virtual-dtor " )
    env.Append( LINKFLAGS=" -fPIC -pthread -rdynamic" )
    env.Append( LIBS=[] )

    env['ENV']['HOME'] = os.environ['HOME']
    env['ENV']['TERM'] = os.environ['TERM']

    if debugBuild:
        env.Append( CPPFLAGS=" -O0 -fstack-protector " );
        env['ENV']['GLIBCXX_FORCE_NEW'] = 1;
        env.Append( CPPFLAGS=" -D_DEBUG" );
    else:
        env.Append( CPPFLAGS=" -O3 " )

try:
    umask = os.umask(022)
except OSError:
    pass

env.Append( CPPPATH=env["EXTRACPPPATH"], LIBPATH=env["EXTRALIBPATH"])

# --- check system ---
def getSysInfo():
    if windows:
        return "windows " + str( sys.getwindowsversion() )
    else:
        return " ".join( os.uname() )

clientCppEnv = env.Clone()
clientCppEnv.Append( CPPDEFINES=[ "SDB_DLL_BUILD" ] )
clientCEnv = clientCppEnv.Clone()
clientCppEnv["BUILD_DIR"] = clientCppVariantDir
clientCEnv["BUILD_DIR"] = clientCVariantDir

# --- append boost library to env ---
if nix:
   for b in boostLibs:
      env.Append ( _LIBFLAGS='${SLIBS}',
                   SLIBS=" " + join(boost_lib_dir,"libboost_" + b + ".a") )

testEnv = env.Clone()
testEnv.Append( CPPPATH=["../"] )

shellEnv = None
shellEnv = env.Clone();

toolEnv = None
toolEnv = env.Clone() ;

fmpEnv = None
fmpEnv = env.Clone() ;

if windows:
    shellEnv.Append( LIBS=["winmm.lib"] )
    #env.Append( CPPFLAGS=" /TP " )

# add engine and client variable
env.Append( CPPDEFINES=[ "SDB_ENGINE" ] )
clientCppEnv.Append( CPPDEFINES=[ "SDB_CLIENT" ] )
clientCEnv.Append( CPPDEFINES=[ "SDB_CLIENT" ] )
# should we use engine or client for test env? not sure, let's put client for now
testEnv.Append( CPPDEFINES=[ "SDB_CLIENT" ] )
shellEnv.Append( CPPDEFINES=[ "SDB_CLIENT" ] )
shellEnv.Append( CPPDEFINES=[ "SDB_SHELL" ] )
toolEnv.Append( CPPDEFINES=[ "SDB_CLIENT" ] )
toolEnv.Append( CPPDEFINES=[ "SDB_TOOL" ] )
toolEnv.Append( CPPPATH=[ncursesinclude_dir] )
fmpEnv.Append( CPPDEFINES=[ "SDB_FMP" ] )
fmpEnv.Append( CPPDEFINES=[ "SDB_CLIENT" ] )

env['INSTALL_DIR'] = installDir
if testEnv is not None:
    testEnv['INSTALL_DIR'] = installDir
if shellEnv is not None:
    shellEnv['INSTALL_DIR'] = installDir
if clientCppEnv is not None:
    clientCppEnv['INSTALL_DIR'] = installDir
if clientCEnv is not None:
    clientCEnv['INSTALL_DIR'] = installDir
if fmpEnv is not None:
    fmpEnv['INSTALL_DIR'] = installDir
# The following symbols are exported for use in subordinate SConscript files.
# Ideally, the SConscript files would be purely declarative.  They would only
# import build environment objects, and would contain few or no conditional
# statements or branches.
#
# Currently, however, the SConscript files do need some predicates for
# conditional decision making that hasn't been moved up to this SConstruct file,
# and they are exported here, as well.
Export("env")
Export("shellEnv")
Export("toolEnv")
Export("testEnv")
Export("fmpEnv")
Export("clientCppEnv")
Export("clientCEnv")
Export("installSetup getSysInfo")
Export("usesm")
Export("windows linux nix")
if usesm:
   Export("smlib_file")
Export("ssllib_file")
Export("ssllib_file1")
Export("hasEngine")
Export("hasTestcase")
Export("hasTool")
Export("driverDir")
Export("guess_os")

# Generating Versioning information
# In order to change the file location, we have to modify both win32 and linux
# ossVer_Autogen.h is NOT in SVN, we have to generate this file by scons before
# actually compling the project
# Thus, we should avoid putting ossVer* files to release package
#
# In github build, we don't have svn info, so we don't run svn or SubWCRev
# command. Instead the svn fork tool should already generated the right
# ossVer_Autogen.h file
if not os.path.isfile ( "gitbuild" ):
   if guess_os == "win32":
      # In windows platform, we take advantage of SubWCRev
      os.system ("SubWCRev . misc/autogen/ossVer.tmp SequoiaDB/engine/include/ossVer_Autogen.h")
   else:
      # In NIX platform, we use svn and sed to send to ossVer_Autogen.h
      os.system("sed \"s/WCREV/$(svn info | grep Revision | awk '{print $2}')/g\" misc/autogen/ossVer.tmp > oss.tmp")
      os.system("sed 's/\$//g' oss.tmp > SequoiaDB/engine/include/ossVer_Autogen.h")

language = get_option ( "language" )
if language is None:
   os.system ( "scons -C misc/autogen" )
else:
   os.system ( "scons -C misc/autogen --language=" + language )

if hasEngine:
   env.SConscript( 'SequoiaDB/SConscript', variant_dir=variantDir, duplicate=False )

# Convert javascript files to a cpp file
print 'Convert js files to cpp'
sys.path.append(join(root_dir, 'misc'))
import jsToCpp
jsToCpp.jsToCpp(engine_dir)

if hasClient:
   clientCppEnv.SConscript( 'SequoiaDB/SConscriptClientCpp', variant_dir=clientCppVariantDir, duplicate=False )
   clientCEnv.SConscript ( 'SequoiaDB/SConscriptClientC', variant_dir=clientCVariantDir, duplicate=False )

if hasShell:
   shellEnv.SConscript ( 'SequoiaDB/SConscriptShell', variant_dir=shellVariantDir, duplicate=False )

if hasTool:
   toolEnv.SConscript ( 'SequoiaDB/SConscriptTool', variant_dir=toolVariantDir, duplicate=False )
if hasFmp:
   fmpEnv.SConscript ( 'SequoiaDB/SConscriptFmp', variant_dir=fmpVariantDir, duplicate=False )
#if hasTestcase:
#   env.SConscript( 'SequoiaDB/SConscript', variant_dir=variantDir, duplicate=False )

