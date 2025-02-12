<?php

namespace ORPTech\MigrationPartition\Database\Schema\Grammars;

use Illuminate\Database\Schema\Grammars\PostgresGrammar as IlluminatePostgresGrammar;
use Illuminate\Support\Fluent;
use ORPTech\MigrationPartition\Database\Schema\Blueprint;

class PostgresGrammar extends IlluminatePostgresGrammar
{
    /**
     * Compile a create table command with its range partitions.
     *
     * @return array
     */
    public function compileCreateRangePartitioned(Blueprint $blueprint, Fluent $command)
    {
        return array_values(array_filter(array_merge([sprintf('create table %s (%s) partition by range (%s)',
            $this->wrapTable($blueprint),
            implode(', ', $this->getColumns($blueprint)),
            $blueprint->rangeKey
        )], $this->compileAutoIncrementStartingValues($blueprint))));
    }

    /**
     * Compile a create table partition command for a range partitioned table.
     *
     * @return array
     */
    public function compileCreateRangePartition(Blueprint $blueprint, Fluent $command)
    {
        return array_values(array_filter(array_merge([sprintf('create table %s_%s partition of %s for values from (\'%s\') to (\'%s\')',
            str_replace('"', '', $this->wrapTable($blueprint)),
            $blueprint->suffixForPartition,
            str_replace('"', '', $this->wrapTable($blueprint)),
            $blueprint->startValue,
            $blueprint->endValue
        )], $this->compileAutoIncrementStartingValues($blueprint))));
    }

    /**
     * Compile an attach partition command for a range partitioned table.
     *
     * @return string
     */
    public function compileAttachRangePartition(Blueprint $blueprint, Fluent $command)
    {
        return sprintf('ALTER table %s attach partition %s for values from (\'%s\') to (\'%s\')',
            str_replace('"', '', $this->wrapTable($blueprint)),
            $blueprint->partitionTableName,
            $blueprint->startValue,
            $blueprint->endValue
        );
    }

    /**
     * Compile a create table command with its list partitions.
     *
     * @return string
     */
    public function compileCreateListPartitioned(Blueprint $blueprint, Fluent $command)
    {
        return sprintf('create table %s (%s) partition by list(%s)',
            $this->wrapTable($blueprint),
            implode(', ', $this->getColumns($blueprint)),
            $blueprint->listPartitionKey
        );
    }

    /**
     * Compile a create table partition command for a list partitioned table.
     *
     * @return string
     */
    public function compileCreateListPartition(Blueprint $blueprint, Fluent $command)
    {
        return sprintf('create table %s_%s partition of %s for values in (\'%s\')',
            str_replace('"', '', $this->wrapTable($blueprint)),
            $blueprint->suffixForPartition,
            str_replace('"', '', $this->wrapTable($blueprint)),
            $blueprint->listPartitionValue,
        );
    }

    /**
     * Compile an attach partition command for a list partitioned table.
     *
     * @return string
     */
    public function compileAttachListPartition(Blueprint $blueprint, Fluent $command)
    {
        return sprintf('alter table %s partition of %s for values in (\'%s\')',
            str_replace('"', '', $this->wrapTable($blueprint)),
            $blueprint->partitionTableName,
            $blueprint->listPartitionValue,
        );
    }

    /**
     * Compile a create table command with its hash partitions.
     *
     * @return array
     */
    public function compileCreateHashPartitioned(Blueprint $blueprint, Fluent $command)
    {
        return array_values(array_filter(array_merge([sprintf('create table %s (%s) partition by hash(%s)',
            $this->wrapTable($blueprint),
            implode(', ', $this->getColumns($blueprint)),
            $blueprint->hashPartitionKey
        )], $this->compileAutoIncrementStartingValues($blueprint))));
    }

    /**
     * Compile a create table partition command for a hash partitioned table.
     *
     * @return string
     */
    public function compileCreateHashPartition(Blueprint $blueprint, Fluent $command)
    {
        return sprintf('create table %s_%s partition of %s for values with (modulus %s, remainder %s)',
            str_replace('"', '', $this->wrapTable($blueprint)),
            $blueprint->suffixForPartition,
            str_replace('"', '', $this->wrapTable($blueprint)),
            $blueprint->hashModulus,
            $blueprint->hashRemainder
        );

    }

    /**
     * Compile an attach partition command for a hash partitioned table.
     *
     * @return string
     */
    public function compileAttachHashPartition(Blueprint $blueprint, Fluent $command)
    {
        return sprintf('alter table %s partition of %s for values with (modulus %s, remainder %s)',
            str_replace('"', '', $this->wrapTable($blueprint)),
            $blueprint->partitionTableName,
            $blueprint->hashModulus,
            $blueprint->hashRemainder,
        );
    }

    /**
     * Get a list of all partitioned tables in the Database.
     *
     * @return string
     */
    public function compileGetPartitions(string $table)
    {
        return sprintf("SELECT inhrelid::regclass as tables
            FROM   pg_catalog.pg_inherits
            WHERE  inhparent = '%s'::regclass;",
            $table,
        );
    }

    /**
     * Get all range partitioned tables.
     *
     * @return string
     */
    public function compileGetAllRangePartitionedTables()
    {
        return "select pg_class.relname as tables from pg_class inner join pg_partitioned_table on pg_class.oid = pg_partitioned_table.partrelid where pg_partitioned_table.partstrat = 'r';";
    }

    /**
     * Get all list partitioned tables.
     *
     * @return string
     */
    public function compileGetAllListPartitionedTables()
    {
        return "select pg_class.relname as tables from pg_class inner join pg_partitioned_table on pg_class.oid = pg_partitioned_table.partrelid where pg_partitioned_table.partstrat = 'l';";
    }

    /**
     * Get all hash partitioned tables.
     *
     * @return string
     */
    public function compileGetAllHashPartitionedTables()
    {
        return "select pg_class.relname as tables from pg_class inner join pg_partitioned_table on pg_class.oid = pg_partitioned_table.partrelid where pg_partitioned_table.partstrat = 'h';";
    }

    /**
     * Compile a detach query for a partitioned table.
     *
     * @return string
     */
    public function compileDetachPartition(Blueprint $blueprint, Fluent $command)
    {
        return sprintf('alter table %s detach partition %s',
            str_replace('"', '', $this->wrapTable($blueprint)),
            $blueprint->partitionTableName
        );
    }
}
