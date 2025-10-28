
# RPA - ZETA

ZETA é um aplicativo desenvolvido para auxiliar produtores rurais na criação de animais, oferecendo treinamentos e conteúdos técnicos e práticos que promovem o aprimoramento de seus conhecimentos.

Este repositório há dois arquivos principais:

**schema_staging.py**: RPA para sincronização das tabelas do 1º ano para o schema *staging* do 2º ano.

**collect_data**: RPA para a sincronização de dados com os bancos PostgreSQL e MongoDB.

## Linguagens utilizadas
Para o desenvolvimento dos agentes foi utilizada a linguagem Python.

## Frameworks
Foi utilizado os seguintes frameworks:
* Selenium

## Estrutura dos arquivos
Para realizar a sincronização dos bancos, primeiro execute o arquivo *schema_staging* para criar as tabelas necessáriaas e atualizar os dados do schema staging. Em seguida, execute o arquivo *collect_data.py* para sincronizar os dados e adicionar a descrição de lei - caso exista - aos bancos PostgreSQL e MongoDB.


## Configuração Inicial
1. Clone o repositório:
```bash
git clone https://github.com/ZETA-Inter/Zeta-RPA.git
```
2. Abra o projeto na IDE de sua preferência;
3. Abra o terminal e instale as dependências do projeto:
```bash
pip install -r requirements.txt
```
4. Execute o arquivo *schema_staging.py*;
5. Em seguida, execute o arquivo *collect_data.py*.

## Desenvolvedores
Desenvolvido com dedicação pela equipe de tecnologia ZETA:
- [Raquel Tolomei](https://github.com/RaquelTolomei)  
- [Sofia Rodrigues Santana](https://github.com/SofiaRSantana)  
- [Sophia Laurindo Gasparetto](https://github.com/sosogasp)  

## Contato
Para mais informações ou suporte, entre em contato através de nosso site ou envie um email para appzetaofc@gmail.com.

## Licença  
[MIT](https://choosealicense.com/licenses/mit/)

## Copyright
© **Copyright ZETA 2025**  

Todos os direitos reservados.
Este software é protegido por leis de direitos autorais. Não é permitida a cópia, distribuição ou modificação sem permissão do autor.






