# MessengerDedupeBundle

A Symfony bundle that prevents duplicate messages from accumulating in the `messenger_messages`
table when using Doctrine transport. Uses a hash-based deduplication middleware and automatic
hash cleanup on message consumption.

Originally developed by [ByteSpin](https://github.com/ByteSpin/MessengerDedupeBundle).
This is a **local vendor fork** with reliability and operational improvements.

## How it works

1. When dispatching a message, you attach a `HashStamp` with a hash that represents the
   message's identity (you decide what "same message" means)
2. The `DeduplicationMiddleware` checks the `messenger_messages_hash` table:
   - Hash exists and not expired → message is silently dropped (deduplication)
   - Hash exists but expired (TTL) → stale hash is cleaned up, message dispatched
   - Hash doesn't exist → hash is persisted, message dispatched
3. When the message is consumed (success or failure), the `MessageHashEventSubscriber`
   deletes the hash from the database

Race conditions are handled via a unique constraint on the hash column — if two processes
try to insert the same hash simultaneously, the second one catches the
`UniqueConstraintViolationException` and drops the message.

## Requirements

- PHP 8.2+
- Symfony 7.1+
- Doctrine ORM

## Fork improvements over upstream

### TTL-based hash expiration

The middleware accepts an optional `ttlSeconds` constructor parameter. When configured,
hashes older than the TTL are treated as stale orphans — they are deleted and the message
is allowed through. This prevents permanent deduplication locks when a message is dispatched
but never consumed (e.g. worker crash, transport misconfiguration).

```yaml
# config/services.yaml
ByteSpin\MessengerDedupeBundle\Middleware\DeduplicationMiddleware:
    arguments:
        $ttlSeconds: 3600  # hashes older than 1 hour are considered expired
```

Without TTL configuration, behaviour is identical to upstream (hashes persist until consumed).

### Bulk hash cleanup

`MessengerMessageHashRepository` includes a `deleteExpiredBefore()` method for efficient
bulk cleanup of orphaned hashes:

```php
$repo->deleteExpiredBefore(new DateTimeImmutable('-1 hour'));
```

Uses a single DQL DELETE (no hydration) for performance. Useful in scheduled cleanup commands.

### EntityManager resilience

The middleware includes `resetEntityManager()` recovery logic. After a
`UniqueConstraintViolationException` closes the EntityManager, it is properly reset via
the `ManagerRegistry` so subsequent middleware calls don't fail. The upstream version
would leave a closed EntityManager after race-condition deduplication.

### Symfony 7 compatibility

- `composer.json` requires `symfony/*: ^7.1` (upstream targets 6.3)
- Entity uses `DateTimeImmutable` for the `createdAt` column
- Event subscriber handles both `WorkerMessageHandledEvent` and `WorkerMessageFailedEvent`
  with a union type signature

## Installation

The bundle is installed as a local vendor fork (not via Packagist).

### 1. Register the bundle

```php
// config/bundles.php
return [
    // ...
    ByteSpin\MessengerDedupeBundle\MessengerDedupeBundle::class => ['all' => true],
];
```

### 2. Configure the entity mapping

Add the entity mapping to your Doctrine configuration:

```yaml
# config/packages/doctrine.yaml
doctrine:
    orm:
        entity_managers:
            default:
                mappings:
                    ByteSpin\MessengerDedupeBundle:
                        is_bundle: false
                        type: attribute
                        dir: '%kernel.project_dir%/vendor/bytespin/messenger-dedupe-bundle/src/Entity'
                        prefix: ByteSpin\MessengerDedupeBundle\Entity
                        alias: MessengerDedupeBundle
```

### 3. Create the hash table

```bash
php bin/console doctrine:schema:update --force
```

### 4. Enable the middleware

```yaml
# config/packages/messenger.yaml
framework:
    messenger:
        buses:
            messenger.bus.default:
                middleware:
                    - ByteSpin\MessengerDedupeBundle\Middleware\DeduplicationMiddleware
```

The deduplication middleware must be listed **first** — before any other middleware.

## Usage

```php
use ByteSpin\MessengerDedupeBundle\Messenger\Stamp\HashStamp;
use ByteSpin\MessengerDedupeBundle\Processor\HashProcessor;

class MyService
{
    public function __construct(
        private MessageBusInterface $messageBus,
        private HashProcessor $hashProcessor,
    ) {}

    public function doSomething(): void
    {
        // Define what makes this message unique
        $hash = $this->hashProcessor->makeHash('ImportProfile' . $profileId . $sourceUrl);

        $this->messageBus->dispatch(
            new Envelope(
                $message,
                [
                    new TransportNamesStamp('async'),
                    new HashStamp($hash),
                ]
            )
        );
    }
}
```

Messages dispatched without a `HashStamp` pass through the middleware untouched.

### Multi-application architecture

For setups where a master application dispatches messages to remote workers, the bundle
provides `MasterStamp` and `InitiatorStamp` for coordinating hash cleanup across
application boundaries. See the stamp classes in `src/Messenger/Stamp/` for details.

## Components

| Class | Role |
|-------|------|
| `DeduplicationMiddleware` | Checks/inserts hashes on dispatch, handles TTL expiration |
| `MessageHashEventSubscriber` | Removes hashes after message consumption (success or failure) |
| `HashProcessor` | SHA-256 hash computation helper |
| `HashStamp` | Envelope stamp carrying the deduplication hash |
| `MessengerMessageHash` | Doctrine entity for the hash storage table |
| `MessengerMessageHashRepository` | Repository with `deleteExpiredBefore()` for bulk cleanup |
| `ActionStamp`, `MasterStamp`, `InitiatorStamp` | Stamps for multi-application setups |

## License

MIT
