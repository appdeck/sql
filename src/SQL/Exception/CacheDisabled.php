<?php
/**
 *
 * Exception thrown when trying to tag a query with a disabled Query Cache
 *
 * @copyright 2014 appdeck
 * @link http://github.com/appdeck/sql
 * @license http://www.gnu.org/licenses/gpl-3.0.html GNU General Public License, version 3
 */

namespace SQL\Exception;

class CacheDisabled extends \Exception {
	protected $message = 'You have to enable cache before tagging a query.';
}