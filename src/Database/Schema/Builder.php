<?php

namespace ORPTech\MigrationPartition\Database\Schema;

use Closure;
use Illuminate\Container\Container;
use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Builder as IlluminateBuilder;
use Illuminate\Support\Facades\DB;
use ORPTech\MigrationPartition\Database\Schema\Grammars\PostgresGrammar;

class Builder extends IlluminateBuilder
{
    public function __construct(Connection $connection)
    {
        parent::__construct($connection);
        $this->grammar = new PostgresGrammar();
    }

    /**
     * Create a new table on the schema with range partitions.
     *
     * @return void
     *
     * @throws BindingResolutionException
     */
    public function createRangePartitioned(string $table, Closure $callback, string $rangeKey)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $rangeKey) {
            $blueprint->createRangePartitioned();
            $blueprint->rangeKey = $rangeKey;

            $callback($blueprint);
        }));
    }

    /**
     * Create a new range partition on the table.
     *
     * @return void
     *
     * @throws BindingResolutionException
     */
    public function createRangePartition(string $table, Closure $callback, string $suffixForPartition, string $startValue, string $endValue)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $suffixForPartition, $startValue, $endValue) {
            $blueprint->createRangePartition();
            $blueprint->suffixForPartition = $suffixForPartition;
            $blueprint->startValue = $startValue;
            $blueprint->endValue = $endValue;

            $callback($blueprint);
        }));
    }

    /**
     * Attach a new range partition to a partitioned table.
     *
     * @return void
     *
     * @throws BindingResolutionException
     */
    public function attachRangePartition(string $table, Closure $callback, string $partitionTableName, string $startValue, string $endValue)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $partitionTableName, $startValue, $endValue) {
            $blueprint->attachRangePartition();
            $blueprint->partitionTableName = $partitionTableName;
            $blueprint->startValue = $startValue;
            $blueprint->endValue = $endValue;
            $callback($blueprint);
        }));
    }

    /**
     * Create a new table on the schema with list partitions.
     *
     * @return void
     *
     * @throws BindingResolutionException
     */
    public function createListPartitioned(string $table, Closure $callback, string $listPartitionKey)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $listPartitionKey) {
            $blueprint->createListPartitioned();
            $blueprint->listPartitionKey = $listPartitionKey;

            $callback($blueprint);
        }));
    }

    /**
     * Create a list partition on the table.
     *
     * @return void
     *
     * @throws BindingResolutionException
     */
    public function createListPartition(string $table, Closure $callback, string $suffixForPartition, string $listPartitionValue)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $suffixForPartition, $listPartitionValue) {
            $blueprint->createListPartition();
            $blueprint->suffixForPartition = $suffixForPartition;
            $blueprint->listPartitionValue = $listPartitionValue;

            $callback($blueprint);
        }));
    }

    /**
     * Attach a new list partition.
     *
     * @return void
     *
     * @throws BindingResolutionException
     */
    public function attachListPartition(string $table, Closure $callback, string $partitionTableName, string $listPartitionValue)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $partitionTableName, $listPartitionValue) {
            $blueprint->attachListPartition();
            $blueprint->partitionTableName = $partitionTableName;
            $blueprint->listPartitionValue = $listPartitionValue;
            $callback($blueprint);
        }));
    }

    /**
     * Create a table on the schema with hash partitions.
     *
     * @return void
     *
     * @throws BindingResolutionException
     */
    public function createHashPartitioned(string $table, Closure $callback,  string $hashPartitionKey)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $hashPartitionKey) {
            $blueprint->createHashPartitioned();
            $blueprint->hashPartitionKey = $hashPartitionKey;
            $callback($blueprint);
        }));
    }

    /**
     * Create and attach a new hash partition on the table.
     *
     * @return void
     *
     * @throws BindingResolutionException
     */
    public function createHashPartition(string $table, Closure $callback, string $suffixForPartition, int $hashModulus, int $hashRemainder)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $suffixForPartition, $hashModulus, $hashRemainder) {
            $blueprint->createHashPartition();
            $blueprint->suffixForPartition = $suffixForPartition;
            $blueprint->hashModulus = $hashModulus;
            $blueprint->hashRemainder = $hashRemainder;
            $callback($blueprint);
        }));
    }

    /**
     * Attach a hash partition.
     *
     * @return void
     *
     * @throws BindingResolutionException
     */
    public function attachHashPartition(string $table, Closure $callback, string $partitionTableName, int $hashModulus, int $hashRemainder)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $partitionTableName, $hashModulus, $hashRemainder) {
            $blueprint->attachHashPartition();
            $blueprint->partitionTableName = $partitionTableName;
            $blueprint->hashModulus = $hashModulus;
            $blueprint->hashRemainder = $hashRemainder;
            $callback($blueprint);
        }));
    }

    /**
     * Get all the partitioned table names for the database.
     *
     * @return array
     */
    public function getPartitions(string $table)
    {
        return array_column(DB::select($this->grammar->compileGetPartitions($table)), 'tables');
    }

    /**
     * Get all the range partitioned table names for the database.
     *
     * @return array
     */
    public function getAllRangePartitionedTables()
    {
        return array_column(DB::select($this->grammar->compileGetAllRangePartitionedTables()), 'tables');
    }

    /**
     * Get all the list partitioned table names for the database.
     *
     * @return array
     */
    public function getAllListPartitionedTables()
    {
        return array_column(DB::select($this->grammar->compileGetAllListPartitionedTables()), 'tables');
    }

    /**
     * Get all the hash partitioned table names for the database.
     *
     * @return array
     */
    public function getAllHashPartitionedTables()
    {
        return array_column(DB::select($this->grammar->compileGetAllHashPartitionedTables()), 'tables');
    }

    /**
     * Detaches a partition from a partitioned table.
     *
     * @throws BindingResolutionException
     */
    public function detachPartition(string $table, Closure $callback, string $partitionTableName): void
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $partitionTableName) {
            $blueprint->detachPartition();
            $blueprint->partitionTableName = $partitionTableName;
            $callback($blueprint);
        }));
    }

    /**
     * Create a new command set with a Closure.
     *
     * @param  string  $table
     * @return Closure|mixed|object|Blueprint|null
     *
     * @throws BindingResolutionException
     */
    protected function createBlueprint($table, Closure $callback = null)
    {
        $prefix = $this->connection->getConfig('prefix_indexes')
            ? $this->connection->getConfig('prefix')
            : '';

        if (isset($this->resolver)) {
            return call_user_func($this->resolver, $table, $callback, $prefix);
        }

        return Container::getInstance()->make(Blueprint::class, compact('table', 'callback', 'prefix'));
    }
}
