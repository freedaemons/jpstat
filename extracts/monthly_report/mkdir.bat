@echo off
SetLocal

set "months=01Jan 02Feb 03Mar 04Apr 05May 06Jun 07Jul 08Aug 09Sep 10Oct 11Nov 12Dec"

FOR /L %%i in (2006,1,2016) DO (
	if not exist %%i (
		mkdir %%i 
		echo "created dir %%i"
	)else(
		echo "dir %%i already exists"
	)
	FOR %%k in (%months%) DO (
		if not exist %%i\%%k (
			mkdir %%i\%%k 
			echo "created dir %%i\%%k"
		)else(
			echo "dir %%i\%%k already exists"
		)
	)
)