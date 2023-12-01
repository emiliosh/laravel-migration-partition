<?php

namespace ORPTech\MigrationPartition\Commands;

use Illuminate\Console\Command;
use ORPTech\MigrationPartition\Support\Facades\Schema;

class ListTablePartitionsCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'partition:partitions';

    /**
     * The console command description.
     */
    protected string $table;

    /**
     * Execute the console command.
     */
    public function handle(): void
    {
        $this->table = $this->ask('Table name');
        $tables = Schema::getPartitions($this->table);
        foreach ($tables as $table) {
            $this->info($table);
        }
    }
}
