# google pubsub

## Visão Geral

Este projeto em Go implementa um sistema de publicação e subscrição (PubSub) utilizando a biblioteca Google PubSub. 
Ele consiste em três componentes principais: PubSub, Publisher e Subscriber.

## Configuração
### Pré-Requisitos
- Go instalado (versão 1.x)
- Variáveis de ambiente configuradas para Google Cloud (PROJECT_ID, PUBSUB_EMULATOR_HOST, etc.)
### Instalação
- Clone o repositório e instale as dependências necessárias.
```sh
Copy code
git clone [URL_DO_REPOSITORIO]
cd [NOME_DO_REPOSITORIO]
go get .
```

## Utilização
### Inicialização do PubSub
Para iniciar uma instância do PubSub, utilize a função ConnectPubSub. Esta função aceita opções de configuração que podem ser customizadas, como LoadFromEnv, para carregar configurações do ambiente.
```go
pubsubInstance, err := ConnectPubSub(LoadFromEnv)
if err != nil {
    log.Fatalf("Erro ao conectar ao PubSub: %v", err)
}
defer pubsubInstance.Close(context.Background())
```

## Publicação de Mensagens
Para publicar mensagens, primeiro crie uma instância do Publisher a partir da instância do PubSub e então utilize o método Send.

```go
publisher := pubsubInstance.Publisher()

msg := &Message{
    Topic:   "seu-topico",
    Content: NewContent().Marshal("my custom message"),
}

err := publisher.Send(context.Background(), msg)
if err != nil {
    log.Fatalf("Erro ao enviar mensagem: %v", err)
}
```
## Subscrição e Processamento de Mensagens
Para subscrever a tópicos e processar mensagens, defina uma função Subscriber e registre-a usando o método Register da instância do Subscription.

```go
subscriptionInstance := pubsubInstance.Subscription()
subscriber := OrderSub(logger)

err := subscriptionInstance.Register(context.Background(), subscriber)
if err != nil {
    log.Fatalf("Erro ao registrar subscriber: %v", err)
}
```
## Encerramento e Liberação de Recursos
Utilize o método Close para fechar conexões e liberar recursos.

```go
err := pubsubInstance.Close(context.Background())
if err != nil {
    log.Fatalf("Erro ao fechar conexões do PubSub: %v", err)
}
```
### Exemplo de Subscriber
```go
func OrderSub(logger Logger) pubsub.Subscriber {
    return pubsub.Subscriber{
        Config: pubsub.SubscriptionConfig{
            Topic:           "order-created",
            SubscriptionID:  "order-created-group",
            PullingQuantity: 10,
            TickerTime:      time.Second,
            EarlyAck:        false,
        },
        Handler: func(ctx context.Context, message *Message) error {
            var expectedValue map[string]any
            if err := message.Content.Unmarshal(&expectedValue); err != nil {
                return err
            }           
            logger.Info(ctx, string(expectedValue))
            return nil
        },
        OnError: func(ctx context.Context, err error, header Header) {
            logger.Error(ctx, err.Error())
        },
    }
}
```

#### Utilize este exemplo para criar um subscriber para o tópico "order-created".