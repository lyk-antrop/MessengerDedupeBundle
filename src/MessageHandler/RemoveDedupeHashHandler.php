<?php

namespace ByteSpin\MessengerDedupeBundle\MessageHandler;

use AllowDynamicProperties;
use ByteSpin\MessengerDedupeBundle\Entity\MessengerMessageHash;
use ByteSpin\MessengerDedupeBundle\Model\RemoveDedupeHash;
use ByteSpin\MessengerDedupeBundle\Repository\MessengerMessageHashRepository;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\Persistence\ManagerRegistry;
use RuntimeException;
use Symfony\Component\Messenger\Attribute\AsMessageHandler;

#[AllowDynamicProperties]
#[AsMessageHandler]
class RemoveDedupeHashHandler
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

    public function __invoke(RemoveDedupeHash $message): void
    {
        if ($message->hash) {
            if ($hashData = $this->hashRepository->findOneBy(['hash' =>$message->hash])) {
                // delete message hash from database
                $this->entityManager->remove($hashData);
                $this->entityManager->flush();
                $this->entityManager->clear();
            }
        }
    }
}
