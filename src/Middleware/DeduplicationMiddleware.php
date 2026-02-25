<?php

/**
 * This file is part of the ByteSpin/MessengerDedupeBundle project.
 * The project is hosted on GitHub at:
 *  https://github.com/ByteSpin/MessengerDedupeBundle.git
 *
 * Copyright (c) Greg LAMY <greg@bytespin.net>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace ByteSpin\MessengerDedupeBundle\Middleware;

use AllowDynamicProperties;
use ByteSpin\MessengerDedupeBundle\Entity\MessengerMessageHash;
use ByteSpin\MessengerDedupeBundle\Messenger\Stamp\HashStamp;
use ByteSpin\MessengerDedupeBundle\Repository\MessengerMessageHashRepository;
use DateTimeImmutable;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\Persistence\ManagerRegistry;
use RuntimeException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;

#[AllowDynamicProperties]
class DeduplicationMiddleware implements MiddlewareInterface
{
    private EntityManagerInterface $entityManager;

    public function __construct(
        private readonly MessengerMessageHashRepository $hashRepository,
        private readonly ManagerRegistry $managerRegistry,
        private readonly ?int $ttlSeconds = null,
    ) {
        $entityManager = $this->managerRegistry->getManagerForClass(MessengerMessageHash::class);

        if (!$entityManager instanceof EntityManagerInterface) {
            throw new RuntimeException('Unexpected EntityManager type');
        }

        $this->entityManager = $entityManager;
    }

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        if ($envelope->last(ReceivedStamp::class)) {
            // If the message has a ReceivedStamp, it means it has been received from transport.
            // In this case, we skip any further processing in this middleware and pass the
            // envelope to the next middleware in the stack for handling.
            return $stack->next()->handle($envelope, $stack);
        }

        if ($envelope->last(HashStamp::class)) {
            $hash = $envelope->last(HashStamp::class)->getHash();

            // Ignore the message if a similar hash is found in the database

            // "Soft" check
            if ($existingHash = $this->hashRepository->findOneBy(['hash' => $hash])) {
                if (!$this->isExpired($existingHash)) {
                    return $envelope;
                }
                // Stale orphaned hash — delete opportunistically and let this dispatch through
                $this->entityManager->remove($existingHash);
                $this->entityManager->flush();
            }

            // Save the hash into the database
            try {
                $hashData = new MessengerMessageHash();
                $hashData->setHash($hash);
                $this->entityManager->persist($hashData);
                $this->entityManager->flush();
            }
            // "Hard" check
            catch (UniqueConstraintViolationException) {
                $this->resetEntityManager();

                return $envelope;
            }
        }
        return $stack->next()->handle($envelope, $stack);
    }

    /**
     * Returns true when the hash is old enough to be treated as a miss.
     * Always false when TTL is not configured.
     */
    private function isExpired(MessengerMessageHash $hash): bool
    {
        if ($this->ttlSeconds === null) {
            return false;
        }

        $age = (new DateTimeImmutable())->getTimestamp() - $hash->getCreatedAt()->getTimestamp();

        return $age >= $this->ttlSeconds;
    }

    /**
     * Recover from a closed EntityManager
     */
    private function resetEntityManager(): void
    {
        if ($this->entityManager->isOpen()) {
            return;
        }

        if ($this->entityManager->getConnection()->isTransactionActive()) {
            $this->entityManager->getConnection()->rollBack();
        }

        $this->managerRegistry->resetManager();
    }
}
