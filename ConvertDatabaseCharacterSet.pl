#!/usr/bin/perl
# 
# Author: Oren Izmirli <dev@izmirli.org>
# Version: 0.01.20110930

use strict;
use warnings;
use DBI;
use Getopt::Long;
use POSIX qw(strftime);

# database connection info
my $dbName = '';
my $dbUser = '';
my $dbPass = '';
my $dbHost = '';

# operation info
my $utf8NotSet = 0; # indicates if initial re-casting as latin1 can be skipd (re-casting needed when columns are already set to UTF8)
my $runSql = 0;
my $debug = 0;
my $verbose = 0;
my $logfile = './databaseCharacterSetConvertion.log';

# 
my %conversion = (
	CHAR		=> 'BINARY',
	TEXT		=> 'BLOB',
	TINYTEXT	=> 'TINYBLOB',
	MEDIUMTEXT	=> 'MEDIUMBLOB',
	LONGTEXT	=> 'LONGBLOB',
	VARCHAR		=> 'VARBINARY',
	);
my $sqlScript = <<EOH;
##
# SQL script to convert a database from latin1 to UTF8
#
# Based on info from the WordPress Codex:
# http://codex.wordpress.org/Converting_Database_Character_Sets
# http://codex.wordpress.org/User:JeremyClarke/exampleSQLForUTF8Conversion
##

EOH
my $usage = <<USAGE;
Usage: $0 [--run_sql --utf8_not_set -d -v --help]
  -r --run_sql
	instad of outputing SQL script, runing it directly [be careful!]
  --utf8_not_set
	Collation of tables/columns is set to latin1 and not to utf8
  -v --verbose
	run in verbose mode
  -d --debug
	run in debug mode
  -? --help
	this message
USAGE
my $covColsCount = 0;
my @convTables = ();

# open log file
open my $logFH, '>>', $logfile or warn "Failed to open '$logfile' - $!\n";

# get options
my $argvHandeled = GetOptions(
	"run_sql"	=> \$runSql,
	"utf8_not_set"	=> \$utf8NotSet,
	"debug"		=> \$debug,
	"verbose"	=> \$verbose,
	"host=s"	=> \$dbHost,
	"name=s"	=> \$dbName,
	"user=s"	=> \$dbUser,
	"password=s"	=> \$dbPass,
	"log=s"		=> \$logfile,
	"help|?"	=> sub { print "$usage\n"; exit; },
	);
die "$usage\n" unless $argvHandeled;

logit("\n\t---------- Starting [runSql: $runSql, DB: $dbName, User: $dbUser] ----------");
&logit("[DEBUG] utf8NotSet: $utf8NotSet, verbose: $verbose, dbHost: $dbHost, dbPass: $dbPass, logfile: $logfile") if $debug;

# connect to DB
my $dsn = "dbi:mysql:${dbName}:${dbHost}:3306";
my $dbh = DBI->connect($dsn, $dbUser, $dbPass);
&logit("Failed to get DBH for $dbName DB - [$DBI::err] $DBI::errstr ($dbh)", 1) unless $dbh && ref $dbh eq "DBI::db";
&logit("Got dbh for $dbName DB [$dbh]") if $debug;

# get all tables
my $tables = $dbh->selectcol_arrayref("SHOW TABLES FROM $dbName");
&logit("Failed to get TABLES FROM $dbName DB - [$DBI::err] $DBI::errstr", 1) if $DBI::err or !$tables;
&logit("Got ".scalar(@$tables)." tables in $dbName DB: ".join(', ', @$tables));

# foreach table, check Column types and convert if needed
foreach my $curTable (@$tables) {
	my $tableColsType = $dbh->selectcol_arrayref("SHOW COLUMNS FROM $curTable IN $dbName", {Columns => [1,2]});
	&logit("Failed to get Column types for $curTable table - [$DBI::err] $DBI::errstr", 1) if $DBI::err or !$tableColsType;
	my %curColsTypes = @$tableColsType;
	&logit(" Table $curTable (".scalar(keys %curColsTypes)." Columns):") if $debug or $verbose;
#print "Table $curTable got these culumns:\n\t" . join("\n\t", map("$_: $curColsTypes{$_}", keys %curColsTypes)) . "\n\n";
	foreach my $curCol (keys %curColsTypes) {
		my ($curType, $charLimit) = $curColsTypes{$curCol} =~ /^(\w+)(?:\W(\d+).*)?$/ if $curColsTypes{$curCol};
		$curType = uc $curType if $curType;
		if($curType and grep($curType eq $_, keys %conversion)) {
			&logit("    $curCol of type $curType will be converted to $conversion{$curType} ($curColsTypes{$curCol})") if $debug;

			my $limit = $curType eq 'VARCHAR' ? "($charLimit)" : '';
			my @curSqlCommends = ();
			push(@curSqlCommends, "alter table `$curTable` change $curCol $curCol $curType$limit CHARACTER SET latin1") unless $utf8NotSet;
			push(@curSqlCommends, "alter table `$curTable` change $curCol $curCol $conversion{$curType}$limit");
			push(@curSqlCommends, "alter table `$curTable` change $curCol $curCol $curType$limit CHARACTER SET utf8");
			
			if($runSql) {
				my $step = 1;
				foreach my $curSql (@curSqlCommends) {
					my $sqlRes = $dbh->do($curSql);
					if($DBI::err) {
						&logit("Failed to convert '$curCol' Column form $curTable table (step: $step) - [$DBI::err] $DBI::errstr\nSQL: $curSql", 1);
					}
					&logit("      $curCol convertion step $step completed successfuly [$sqlRes]") if $debug;
					$step++;
				}
			} else {
				$sqlScript .= "\n## $curTable Table ##\n" unless grep($_ eq $curTable, @convTables);
				$sqlScript .= "# $curCol Column\n";
				$sqlScript .= join(";\n", @curSqlCommends) . ";\n";
			}
			
			$covColsCount++;
			push(@convTables, $curTable) unless grep($_ eq $curTable, @convTables);
		} else {
			&logit("    - $curCol won't be converted ($curType | $curColsTypes{$curCol})") if $debug;
		}
	}
}

if(!$runSql) {
	print $sqlScript;
	&logit("***** Ended - SQL script ouputed ($covColsCount columns in ".scalar(@convTables)." tables will be converted) *****");
} else {
	print &logit("***** Ended - converted $covColsCount columns in ".scalar(@convTables)." tables".($debug ? ' ('.join(', ', @convTables) .')' : '.')."\n");
}
close $logFH if $logFH;

exit;


sub logit {
	my ($msg, $die) = @_;
	return '' unless $msg or $die;
	$msg ||= '';
	$die = $die ? " [FATAL ERROR]" : '';
	print $logFH "[" . strftime("%Y-%m-%d %H:%M:%S", localtime) . "]$die $msg\n" if $logFH;
	die $msg if $die;
	warn "[" . strftime("%Y-%m-%d %H:%M:%S", localtime) . "] $msg\n" if $verbose;
	return $msg;
}

__END__
