ETW Tracing Manifest
================================

ETW tracing is used in this library. It is constructed based on the example found 

http://blogs.msdn.com/b/seealso/archive/2011/06/08/use-this-not-this-logging.aspx

To modify EBTRacing.man, Run the Manifest Generator (ecmangen.exe) tool from the Windows SDK,

The following step is added to the prebuild event of the project. it requires ETWTracing.*
files to be writable. So please do not check those files into repository.

mc -um EBTracing.man -h $(ProjectDir) -z ETWTracing


Counters
=================================

Performance counters are specified in file Counters.xml. Before compiling, run
powershell script CounterGen.ps1 to generate Counters.hpp and Counters.cpp.

Currently having difficulty adding it into the build process, have to do it
manually.
