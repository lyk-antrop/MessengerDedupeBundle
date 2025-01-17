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

namespace ByteSpin\MessengerDedupeBundle\EventSubscriber;

use AllowDynamicProperties;
use ByteSpin\MessengerDedupeBundle\Entity\MessengerMessageHash;
use ByteSpin\MessengerDedupeBundle\Messenger\Stamp\HashStamp;
use ByteSpin\MessengerDedupeBundle\Repository\MessengerMessageHashRepository;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\Persistence\ManagerRegistry;
use RuntimeException;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\Event\WorkerMessageHandledEvent;

#[AllowDynamicProperties]
class MessageHashEventSubscriber implements EventSubscriberInterface
{
    private EntityManagerInterface $entityManager;

    public function __construct(
        private readonly MessengerMessageHashRepository $hashRepository,
        private readonly ManagerRegistry $managerRegistry,
    ) {
        $entityManager = $this->managerRegistry->getManagerForClass(MessengerMessageHash::class);

        if (!$entityManager instanceof EntityManagerInterface) {
            throw new RuntimeException('Unexpected EntityManager type');
        }

        $this->entityManager = $entityManager;
    }

    public static function getSubscribedEvents(): array
    {
        return [
            WorkerMessageHandledEvent::class => 'onMessageProcessed',
            WorkerMessageFailedEvent::class => 'onMessageProcessed',
        ];
    }

    public function onMessageProcessed(WorkerMessageHandledEvent|WorkerMessageFailedEvent $event): void
    {
        $envelope = $event->getEnvelope();
        $hashStamp = $envelope->last(HashStamp::class);
        if ($hashStamp) {
            $hash = $hashStamp->getHash();
            if ($hashData = $this->hashRepository->findOneBy(['hash' => $hash])) {
                // delete message hash from  database
                $this->entityManager->remove($hashData);
                $this->entityManager->flush();
            }
        }
    }
}
