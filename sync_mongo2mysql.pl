#!/usr/bin/perl

use lib "$ENV{'HOME'}/perl5/lib/perl5";
use local::lib;

use MongoDB::OID;

use warnings;

use strict;

my $version = '0.1';

use utf8;
use DBI;
use MongoDB;
use MongoDB::OID;
use Config::General;
use Encode;

my ($G_mdb, $G_dbh, %G_config);

my %mysql_illegal_fieldnames = (insert => '');

main();

# ================================================ end main ========================================
# ================================================ end main ========================================
# ================================================ end main ========================================

use Getopt::Long;

our (
    $opt_verbose, $G_verbose_mode, $G_test
);

my $G_version = '0.1';

sub main {
    my ($config_file, $mysql_host, $mysql_db, $mysql_user, $mysql_pw, $mongo_db);

    GetOptions(
        'c|config=s'  => \$config_file,
        'v|version'   => sub{print "\nsync_mongo2mysql.pl version: $G_version\n\n"; exit},
        'h|help|?'    => \&usage,
    );

    if($config_file eq '')  {
        $config_file = 'sync_mongo2mysql.cfg';
    }

    get_config($config_file);

    connect_mongo();
    connect_mysql();

    my %collections = (weatherData => {});

    load_mongo_data(\%collections);
}

sub load_mongo_data {
    my $collections = shift;

    my $get_colums = 'SHOW columns FROM weatherData';

    my %legal_fields = ();
    my $sth = $G_dbh->prepare($get_colums);
    $sth->execute();
    while (my $ref = $sth->fetchrow_hashref()) {
        $legal_fields{$ref->{'Field'}} = 1;
    }

    foreach my $collection_name (keys %$collections) {
        my $collection = $G_mdb->get_collection($collection_name);

        truncate_table($collection_name);

        my $query_result = $collection->find($collections->{ $collection_name });

        my $ins_count = 0;
        while (my $next = $query_result->next) {

            my $field_list = '';
            my @value_list = ();
            foreach my $field (keys %$next) {
                if (exists $mysql_illegal_fieldnames{ $field}) {
                    $field .= '1';
                }

                if (not exists $legal_fields{$field}) {
                    warn "Missing field '$field' in table '$collection_name' ";
                    next;
                }

                my $data = $next->{$field};

                my $typ = ref $data;
                if ($typ eq 'DateTime') {
                    $data = $next->{$field}->iso8601() . 'Z';
                }

                $field_list .= "$field , ";
                push @value_list, $data;
            }

            $field_list =~ s/ , $//;
            insert_record_in_mysql_table( $field_list, \@value_list, $collection_name);
            $ins_count++;
        }

        print "Collection: '$collection_name' $ins_count rows inserted in mysql database\n";
    }
}

sub truncate_table{
    my $tablename = shift;

    eval {
        $G_dbh->do( "truncate table $tablename" ); # $rows_proceed = 1 -> update hat geklappt
    };

    # ??? DB Zugriff schief gegangen ???
    if ($@) {
        warn "trucate error $@";
    }

}

sub insert_record_in_mysql_table {
    my $fieldlist = shift;
    my $valuelist = shift;
    my $tablename = shift;

    my $value_dummies = '?, ' x @$valuelist;
    $value_dummies =~ s/, $//;

    my $insert_sql = "insert into $tablename ( $fieldlist ) values ( $value_dummies )";

    my $rows_proceed = 0;
    eval {
        $rows_proceed = $G_dbh->do( $insert_sql, undef, @$valuelist ); # $rows_proceed = 1 -> update hat geklappt
    };

    # ??? DB Zugriff schief gegangen ???
    if ($@) {
        warn "insert error $@";
    }

    #print "status: '$@'\n"
}

sub connect_mysql {

    my $db_properties = "DBI:mysql:database=$G_config{mysql_db};host=$G_config{mysql_host}";

    $G_dbh = DBI->connect(
        $db_properties,
        $G_config{mysql_user},
        $G_config{mysql_pw},
        { AutoCommit => 1 }
    );

    $G_dbh->{'mysql_enable_utf8'} = 1;
}

sub connect_mongo {
    my $client = MongoDB::MongoClient->new;

    $G_mdb = $client->get_database($G_config{mongo_db});
}

sub get_config {
    my $config_file = shift;

    my $conf = new Config::General(
        -ConfigFile            => $config_file,
        -InterPolateVars       => 1,
        -MergeDuplicateOptions => 1,
        -MergeDuplicateBlocks  => 1,
    );

    %G_config = $conf->getall;

    my @pflicht_parameter = qw(
        mysql_db mysql_user mysql_pw mongo_db
    );

    my @missing_parameter;

    foreach my $parametername (@pflicht_parameter) {

        if ( !exists $G_config{$parametername} or $G_config{$parametername} =~ /^\s*$/ ) {
            push @missing_parameter, $parametername;
        }
    }

    if (@missing_parameter) {
        warn "Following parameter are required in configfile '$config_file':\n\n"
            . eval { join "\n", @missing_parameter }
            . "\n";
        exit 1;
    }

    if ( !defined( $G_config{mysql_host} ) ) {
        $G_config{mysql_host} = 'localhost';
    }
}

