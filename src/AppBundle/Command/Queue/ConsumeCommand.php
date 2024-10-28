<?php

namespace AppBundle\Command\Queue;

use Amara\Component\MessageQueue\Bridge\Monitoring\Enum\MetricEnum;
use Amara\Component\MessageQueue\Enum\OptionsKeysEnum;
use Amara\Component\MessageQueue\MessagesConsumerInterface;
use Amara\Component\MessageQueue\QueueNameResolverInterface;
use Amara\Component\Monitoring\Enum\MetricUnitEnum;
use Amara\Component\Monitoring\MetricPublisherInterface;
use Amara\Component\Monitoring\Model\Metric;
use AppBundle\Command\ConsoleCommand;
use AppBundle\Enum\MessageQueue\MessageQueueEnum;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Stopwatch\StopwatchEvent;

class ConsumeCommand extends ConsoleCommand implements ConsumeCommandInterface
{
    protected static $defaultName = 'amara:queue-consume';

    /**
     * @var MessagesConsumerInterface
     */
    private $messagesConsumer;

    /**
     * @var QueueNameResolverInterface
     */
    private $queueNameResolver;

    /**
     * @var MetricPublisherInterface
     */
    private $metricPublisher;

    public function __construct(MessagesConsumerInterface $messagesConsumer, QueueNameResolverInterface $queueNameResolver, MetricPublisherInterface $metricPublisher)
    {
        $this->messagesConsumer = $messagesConsumer;
        $this->queueNameResolver = $queueNameResolver;
        $this->metricPublisher = $metricPublisher;

        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->setDescription('Consumes messages from jobs queue.')
            ->addArgument(
                'queue',
                InputArgument::REQUIRED,
                \sprintf('Name of queue that will be consumed. One of [%s].', \implode(', ', MessageQueueEnum::getValues()))
            )
            ->addOption(
                'max-runtime',
                null,
                InputOption::VALUE_REQUIRED,
                'Max time for consumer to run.'
            )
            ->addOption(
                'max-consumed-messages',
                null,
                InputOption::VALUE_REQUIRED,
                'Max message to consume.'
            )
            ->addOption(
                'consume-until-queue-empty',
                null,
                InputOption::VALUE_NONE,
                'Should we stop consumer when queue becomes empty?'
            )
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $this->startProfiling();

        $queueName = $input->getArgument('queue');
        $messagesConsumed = $this->messagesConsumer->consume(
            $queueName,
            null,
            $this->mapOptionsFromInput($input)
        );

        $event = $this->stopProfiling();

        $this->publishMetrics($queueName, $messagesConsumed, $event);

        return 0;
    }

    private function publishMetrics(string $queueName, int $messagesConsumed, StopwatchEvent $event): void
    {
        $runtimeSeconds = \round($event->getDuration() / 1000, 2);

        $resolvedQueueName = $this->queueNameResolver->resolve($queueName);
        $this->metricPublisher->publish([
            new Metric(
                MetricEnum::METRIC_NAMESPACE_AMARA_MESSAGE_QUEUE,
                MetricEnum::METRIC_NAME_CONSUMER_MESSAGES_CONSUMED,
                ['QueueName' => $resolvedQueueName],
                $messagesConsumed
            ),
            new Metric(
                MetricEnum::METRIC_NAMESPACE_AMARA_MESSAGE_QUEUE,
                MetricEnum::METRIC_NAME_CONSUMER_RUNTIME,
                ['QueueName' => $resolvedQueueName],
                $runtimeSeconds,
                MetricUnitEnum::SECONDS
            ),
        ]);
    }

    private function mapOptionsFromInput(InputInterface $input): array
    {
        $options = [];

        $maxRuntime = $input->getOption('max-runtime');
        if (null !== $maxRuntime) {
            $options[OptionsKeysEnum::MAX_CONSUMER_RUNTIME] = $maxRuntime;
        }

        $maxConsumedMessages = $input->getOption('max-consumed-messages');
        if (null !== $maxConsumedMessages) {
            $options[OptionsKeysEnum::MAX_CONSUMED_MESSAGES] = (int) $maxConsumedMessages;
        }

        $consumeUntilQueueEmpty = (bool) $input->getOption('consume-until-queue-empty');
        if (null !== $consumeUntilQueueEmpty) {
            $options[OptionsKeysEnum::CONSUME_UNTIL_QUEUE_EMPTY] = $consumeUntilQueueEmpty;
        }

        return $options;
    }
}
