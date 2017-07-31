<?php
namespace Bizat\Drupal\BaseSynchronizer;

abstract class BaseSynchronizerService
{

    protected $_tables = array(); // table name

    /*
     * $_tableSyncSteps: array of number of entries per run or "once" for all entries at once
     *  array of step' per run: number of entries or "once" for all entires at once)
     *
     */
    protected $_tableSyncSteps = array();


    public function addSyncTable($tableName, $entryPerRun, $option = NULL)
    {
        $this->_tables[] = $tableName;
        $this->_tableSyncSteps[$tableName] = $entryPerRun;
    }

    public function __construct()
    {
        if (empty($this->_tables)) {
            throw new Exception(t("addSyncTable must be used to add sync table at least once!"));
        }

    }

    /*
     * Return pair of "method" and "sync_options"
     */
    public function getBatchOperations($sync_options)
    {
        $operations = array();
        $sync_options['timestamp'] = time();
        $operations[] = array('method' => 'initializeBatch', 'sync_options' => $sync_options);

        // each table copy use processBatch
        // each function has it own sandbox and sandbox get cleared when each function set finished = 1
        // start from will get increased for each iteration.
        foreach ($this->_tables as $t) {
            $sync_options['syncing_table'] = $t;
            $sync_options['start_from'] = 0;
            $operations[] = array('method' => 'processBatch', 'sync_options' => $sync_options);
        }
        unset($sync_options['start_from']);
        unset($sync_options['syncing_table']);
        $operations[] = array('method' => 'finishBatch', 'sync_options' => $sync_options);
        return $operations;
    }

    public function initializeBatch(&$context)
    {
        $message = $this->_prepareDestination(
            $context['sandbox']['sync_options']['from'],
            $context['sandbox']['sync_options']['to'],
            $context['sandbox']['sync_options']['timestamp']);
        db_set_active();
        $context['results'][] = __CLASS__ . '->' . __METHOD__ . ' : ' . $message;
        $context['message'] = $message;
        $context['sandbox']['syncing_table'] = $this->_tables[0];
        $context['finished'] = 1;

    }

    public function processBatch(&$context)
    {
        $sync_from = $context['sandbox']['sync_options']['from'];
        $sync_to = $context['sandbox']['sync_options']['to'];

        //file_put_contents("/tmp/batch.txt", "\n##############".$sync_from.' TO '.$sync_to, FILE_APPEND);


        $table = $context['sandbox']['sync_options']['syncing_table'];
        $start_from = $context['sandbox']['sync_options']['start_from'];
        $step_per_run = $this->_tableSyncSteps[$table];

        $dest_table = $table . '_sync_' . $context['sandbox']['sync_options']['timestamp'];
        $total = $this->_getDataCount($sync_from, $table);
        $run = $this->_transferData($sync_from, $table, $sync_to, $dest_table, $start_from, $step_per_run);

        db_set_active();
        $context['sandbox']['sync_options']['start_from'] = $start_from + $run;
        $context['finished'] = ($start_from + $step_per_run) / $total;
        $context['message'] = t("Created @row row(s) of @total in @table",
            array('@row' => ($start_from + $run), '@total' => $total, '@table' => $dest_table));
        if ($context['finished'] >= 1) {
            // process to next operation(maybe next table)
            $context['finished'] = 1;
        }

    }

    public function finishBatch(&$context)
    {
        $sync_to = $context['sandbox']['sync_options']['to'];

        foreach ($this->_tables as $table) {
            $backup_table = $table . '_' . date("YmdHis");
            $sync_table = $table . '_sync_' . $context['sandbox']['sync_options']['timestamp'];
            $this->_renameTable($sync_to, $table, $backup_table);
            $this->_renameTable($sync_to, $sync_table, $table);
            $context['results'][] = t("Created back up in @table", array('@table' => $backup_table));
        }
        $context['results'][] = t("New tables are now active: @tables", array('@tables' => implode(",", $this->_tables)));
        $context['message'] = 'New data are now active';
        db_set_active();

        $context['finished'] = 1;
    }

    public function _prepareDestination($sync_from, $sync_to, $timestamp)
    {
        //file_put_contents("/tmp/batch.txt", "#######33\n\n".var_export($this->_tableSyncOptions,true), FILE_APPEND);
        foreach ($this->_tables as $table) {
            $dest_table = $table . '_sync_' . $timestamp;
            $this->_createTable($sync_from, $table, $sync_to, $dest_table);
        }
        return t("Created tables in @syncto with suffix '_sync_@timestamp'",
            array('@syncto' => $sync_to, '@timestamp' => $timestamp));
    }


    protected function _getCreateTableQuery($sync_from, $table, $dest_table)
    {
        db_set_active($sync_from);
        $result = db_query("SHOW CREATE TABLE " . $table);
        return str_replace($table, $dest_table, $result->fetchField(1));
    }

    protected function _createTable($sync_from, $table, $sync_to, $dest_table)
    {
        $sqlCreate = $this->_getCreateTableQuery($sync_from, $table, $dest_table);
        db_set_active($sync_to);
        return db_query($sqlCreate);
    }

    protected function _getDataCount($src, $table, $options = array())
    {
        db_set_active($src);
        $result = db_query("SELECT COUNT(1) FROM " . $table);
        return $result->fetchField(0);
    }

    protected function _renameTable($src, $from_table, $to_table)
    {
        db_set_active($src);
        db_query("RENAME TABLE " . $from_table . " TO " . $to_table);
    }

    protected function _transferData($sync_from, $table, $sync_to, $dest_table, $start_from, $step_per_run, $options = array())
    {
        db_set_active($sync_from);
        $result = db_query("SELECT * FROM " . $table . " LIMIT " . $start_from . "," . $step_per_run);

        db_set_active($sync_to);
        db_query("SET SESSION sql_mode='ALLOW_INVALID_DATES' ");

        $c = 0;

        $row = $result->fetchAssoc();
        if ($row) {
            $query = db_insert($dest_table)->fields(array_keys($row));
            do {
                $query->values($row)->execute();
                $c++;
            } while ($row = $result->fetchAssoc());
            $query->execute();
        }
        return $c;
    }
}
