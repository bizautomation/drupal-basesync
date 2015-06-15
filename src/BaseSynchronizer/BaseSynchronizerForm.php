<?php

namespace Bizat\Drupal\BaseSynchronizer;

use Drupal\Core\Form\FormBase;
use Drupal\Core\Form\FormStateInterface;

abstract class BaseSynchronizerForm extends FormBase {


    public function __construct() {
        parent::__construct();
    }

    /**
     * Processes the config import batch and persists the importer.
     *
     * @param string $op_method
     *   The synchronization step to do.
     * @param array $sync_options
     *   The synchronization options (will be stored in sandbox)
     * @param $context
     *   The batch context.
     */
    public static function processBatch($op_method, $sync_options, &$context)
    {
        //file_put_contents("/tmp/batch.txt", "\n\n== BEGIN === ".$op_method. " >>>>> ".json_encode($context['sandbox']), FILE_APPEND);
        try {
            // 'sandbox' (read / write): An array that can be freely used to
            //   store persistent data between iterations. It is recommended to
            //   use this instead of $_SESSION, which is unsafe if the user
            //   continues browsing in a separate window while the batch is processing.
            // NOTICE: sandbox is cleared for every iteration
            // - FunctionA --> FunctionA --> (if FunctionA set finished = 1 sandbox is cleared) --> NextFunctionB --> ...
            // FIRST RUN
            if (!$context['sandbox']['sync_options']) {
                $context['sandbox']['sync_options'] = $sync_options;
            }
            $service = \Drupal::service($sync_options['service']);
            call_user_func_array(array($service, $op_method), array($context));

        } catch (\Exception $e) {
            if (!isset($context['results']['errors'])) {
                $context['results']['errors'] = array();
            }
            //file_put_contents("/tmp/batch.txt", "\n\n XXX ---###!! ".$e->getMessage(), FILE_APPEND);
            $context['results']['errors'][] = $e->getMessage();
        }
        //file_put_contents("/tmp/batch.txt", "\n\n== END === ".json_encode($context['sandbox']), FILE_APPEND);
    }

    /**
     * Finish batch.
     *
     */
    public static function finishBatch($success, $results, $operations)
    {
        if ($success) {
            if (!empty($results['errors'])) {
                foreach ($results['errors'] as $error) {
                    drupal_set_message($error, 'error');
                    \Drupal::logger('config_sync')->error($error);
                }
                drupal_set_message(\Drupal::translation()->translate('The database was synchronized with errors.'), 'warning');
            } else {
                drupal_set_message(\Drupal::translation()->translate('The database was synchronized successfully.'), 'warning');

            }
        } else {
            // An error occurred.
            // $operations contains the operations that remained unprocessed.
            $error_operation = reset($operations);
            $message = \Drupal::translation()->translate('An error occurred while processing %error_operation with arguments: @arguments', array('%error_operation' => $error_operation[0], '@arguments' => print_r($error_operation[1], TRUE)));
            drupal_set_message($message, 'error');
        }
    }

}
