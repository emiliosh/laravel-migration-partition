<?php

namespace ORPTech\MigrationPartition;

use ORPTech\MigrationPartition\Commands\InitHashPartitionCommand;
use ORPTech\MigrationPartition\Commands\InitListPartitionCommand;
use ORPTech\MigrationPartition\Commands\InitRangePartitionCommand;
use ORPTech\MigrationPartition\Commands\ListTablePartitionsCommand;
use Spatie\LaravelPackageTools\Package;
use Spatie\LaravelPackageTools\PackageServiceProvider;

class MigrationPartitionServiceProvider extends PackageServiceProvider
{
    public function configurePackage(Package $package): void
    {
        $package->name('laravel-migration-partition')
            ->hasCommands([
                InitRangePartitionCommand::class,
                InitListPartitionCommand::class,
                InitHashPartitionCommand::class,
                ListTablePartitionsCommand::class,
            ]);
    }
}
