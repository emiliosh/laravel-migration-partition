<?php

namespace ORPTech\MigrationPartition\Database\Schema;

use Closure;
use Illuminate\Container\Container;
use Illuminate\Database\Connection;
use ORPTech\MigrationPartition\Database\Schema\Grammars\PostgresGrammar;
use Illuminate\Database\Schema\Builder as IlluminateBuilder;

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
     * @param string $table
     * @param \Closure $callback
     * @param string $pkCompositeOne
     * @param string $pkCompositeTwo
     * @param string $rangeKey
     * @return void
     */
    public function createRangePartitioned($table, Closure $callback, string $pkCompositeOne, string $pkCompositeTwo, string $rangeKey)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $pkCompositeOne, $pkCompositeTwo, $rangeKey) {
            $blueprint->createRangePartitioned();
            $blueprint->pkCompositeOne = $pkCompositeOne;
            $blueprint->pkCompositeTwo = $pkCompositeTwo;
            $blueprint->rangeKey = $rangeKey;

            $callback($blueprint);
        }));
    }

    /**
     * Create a new range partition on the table.
     *
     * @param string $table
     * @param \Closure $callback
     * @param string $subfixForPartition
     * @param string $startDate
     * @param string $endDate
     * @return void
     */
    public function attachRangePartition($table, Closure $callback, string $subfixForPartition, string $startDate, string $endDate)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $subfixForPartition, $startDate, $endDate) {
            $blueprint->attachRangePartition();
            $blueprint->subfixForPartition = $subfixForPartition;
            $blueprint->startDate = $startDate;
            $blueprint->endDate = $endDate;

            $callback($blueprint);
        }));
    }

    /**
     * Create a new table on the schema with list partitions.
     *
     * @param string $table
     * @param \Closure $callback
     * @param string $listPartitionKey
     * @return void
     */
    public function createListPartitioned($table, Closure $callback, string $listPartitionKey)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $listPartitionKey) {
            $blueprint->createListPartitioned();
            $blueprint->listPartitionKey = $listPartitionKey;

            $callback($blueprint);
        }));
    }

    /**
     * Create a new list partition on the table.
     *
     * @param string $table
     * @param \Closure $callback
     * @param string $subfixForPartition
     * @param string $listPartitionValue
     * @return void
     */
    public function attachListPartition($table, Closure $callback, string $subfixForPartition, string $listPartitionValue)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $subfixForPartition, $listPartitionValue) {
            $blueprint->attachListPartition();
            $blueprint->subfixForPartition = $subfixForPartition;
            $blueprint->listPartitionValue = $listPartitionValue;

            $callback($blueprint);
        }));
    }

    /**
     * Create a new table on the schema with hash partitions.
     *
     * @param string $table
     * @param \Closure $callback
     * @param string $hashPartitionKey
     * @return void
     */
    public function createHashPartitioned($table, Closure $callback, string $hashPartitionKey)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $hashPartitionKey) {
            $blueprint->createHashPartitioned();
            $blueprint->hashPartitionKey = $hashPartitionKey;

            $callback($blueprint);
        }));
    }

    /**
     * Create a new hash partition on the table.
     *
     * @param string $table
     * @param \Closure $callback
     * @param string $subfixForPartition
     * @param string $hashModulus
     * @param string $hashRemainder
     * @return void
     */
    public function attachHashPartition($table, Closure $callback, string $subfixForPartition, string $hashModulus, string $hashRemainder)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $subfixForPartition, $hashModulus, $hashRemainder) {
            $blueprint->attachHashPartition();
            $blueprint->subfixForPartition = $subfixForPartition;
            $blueprint->hashModulus = $hashModulus;
            $blueprint->hashRemainder = $hashRemainder;

            $callback($blueprint);
        }));
    }

    /**
     * Create a new command set with a Closure.
     *
     * @param  string  $table
     * @param  \Closure|null  $callback
     * @return
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
