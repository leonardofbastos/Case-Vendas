
-- Tabelas banco de dados:

-- tabela de importação
CREATE TABLE IF NOT EXISTS arz_importar_vendas (
        id_item INT AUTO_INCREMENT PRIMARY KEY,
        cd_order VARCHAR(200),
        ts_order VARCHAR(200),
        cd_customer VARCHAR(200),
        ds_store VARCHAR(200),
        ds_province VARCHAR(200),
        cd_sku VARCHAR(200),
        ds_category VARCHAR(200),
        vl_full_price DECIMAL(18,4),
        vl_price DECIMAL(18,4),
        vl_cost DECIMAL(18,4),
        qt_ordered_units INT,
        qt_returned_units INT
    )

-------------------------------------------------------------------

-- VIEWS:

-- dimensão cliente
create or replace view vw_arz_cliente as
select
	c.*,
	a.qualificacao,
	a.nro_clientes 
from (
		select distinct 
			substring_index(a.cd_customer,'.',1) as codigo_cliente
		from arz_importar_vendas a
		where cd_customer is not null 
)c left join vw_arz_analise_cliente a on a.codigo_cliente = c.codigo_cliente



-- dimensão loja
create or replace view vw_arz_loja as
select distinct 
	1 as nro_lojas,
	a.ds_store as loja,
	a.ds_province as provincia,
	concat(a.ds_store, ' - ', a.ds_province) as ident_loja
from arz_importar_vendas a
where 1=1
and a.ds_store is not null 



-- dimensão produto
create or replace view vw_arz_produto as
select
	x.sku,
	coalesce(x.categoria, 'Não Informada') as categoria,
	1 as nro_produtos
from (
		select 
			row_number()over(partition by a.sku order by a.categoria desc) as pos,
			a.*
		from (
				select distinct 
					a.cd_sku as sku,
					a.ds_category as categoria
				from arz_importar_vendas a
				where 1=1
				and a.cd_sku is not null 
		)a
		where 1=1
)x
where 1=1
and x.pos = 1



-- fato pedido 
create or replace view vw_arz_pedido as
select 
	x.*,
	--
	case when x.vlr_desconto <> 0 then 'S' else 'N' end as possui_desconto,
	case when x.vlr_desconto <> 0 then x.vlr_venda else 0 end as vlr_venda_com_desconto,
	case when x.vlr_desconto = 0 then x.vlr_venda else 0 end as vlr_venda_sem_desconto,
	--
	x.vlr_venda - x.vlr_cmv as lucro_bruto,
	round((((x.vlr_venda - x.vlr_cmv) / x.vlr_venda) * 100), 3) as margem_bruta
	--
from (
		select
		a.*,
			--
			case when a.tipo_movimento = 'Venda' then a.quantidade 
				 when a.tipo_movimento = 'Devoluçao' then a.quantidade * -1
				 else null 
			end as qtd_venda,
			--
			case when a.tipo_movimento = 'Venda' then a.preco_unit * a.quantidade 
				 when a.tipo_movimento = 'Devolução' then (a.preco_unit * a.quantidade) *-1
				 else null 
			end as vlr_venda,
			--
			case when a.tipo_movimento = 'Venda' then a.preco_bruto * a.quantidade 
				 when a.tipo_movimento = 'Devolução' then (a.preco_bruto * a.quantidade) *-1
				 else null 
			end as vlr_venda_bruta,
			--
			case when a.tipo_movimento = 'Venda' then 0
				 when a.tipo_movimento = 'Devolução' then a.quantidade
				 else null 
			end as qtd_devolucao,
			--
			case when a.tipo_movimento = 'Venda' then 0
				 when a.tipo_movimento = 'Devolução' then (a.preco_unit * a.quantidade)
				 else null 
			end as vlr_devolucao,
			--
			case when a.tipo_movimento = 'Venda' and a.preco_bruto <= a.preco_unit then 0
				 when a.tipo_movimento = 'Venda' and a.preco_bruto > a.preco_unit then (a.preco_bruto - a.preco_unit) * a.quantidade
				 when a.tipo_movimento = 'Devolução' and a.preco_bruto <= a.preco_unit then 0
				 when a.tipo_movimento = 'Devolução' and a.preco_bruto > a.preco_unit then ((a.preco_bruto - a.preco_unit) * a.quantidade) * -1
				 else null 
			end as vlr_desconto,
			--
			case when a.tipo_movimento = 'Venda' and a.preco_bruto >= a.preco_unit then 0
				 when a.tipo_movimento = 'Venda' and a.preco_bruto < a.preco_unit then (a.preco_unit - a.preco_bruto) * a.quantidade
				 when a.tipo_movimento = 'Devolução' and a.preco_bruto >= a.preco_unit then 0
				 when a.tipo_movimento = 'Devolução' and a.preco_bruto < a.preco_unit then ((a.preco_unit - a.preco_bruto) * a.quantidade) * -1
				 else null 
			end as vlr_acrescimo,
			--
			case when a.tipo_movimento = 'Venda' then a.preco_custo * a.quantidade
				 when a.tipo_movimento = 'Devolução' then (a.preco_custo * a.quantidade) * -1
			end as vlr_cmv
			--
		from (
				select
					a.id_item,
					a.cd_order as pedido,
					--
					case when a.qt_ordered_units > 0 and a.qt_returned_units = 0 then 'Venda'
						 when a.qt_ordered_units = 0 and a.qt_returned_units < 0 then 'Devolução'
						else 'Avaliar'
					end as tipo_movimento,
					--
					STR_TO_DATE(substring(a.ts_order, 1, 10), '%Y-%m-%d') as data_emissao,
					STR_TO_DATE(a.ts_order, '%Y-%m-%d %H:%i:%s') as data_hora_emissao,
					--
					coalesce(substring_index(a.cd_customer,'.',1), 'Não Informado') as codigo_cliente,
					case when a.cd_customer is null then 'N' else 'S' end as informado_cliente,
					--
					a.ds_store as loja,
					case when a.ds_store is null then 'N' else 'S' end as informado_loja,
					concat(a.ds_store, ' - ', a.ds_province) as ident_loja,
					--
					a.cd_sku as sku,
					case when a.cd_sku is null then 'N' else 'S' end as informado_produto,
					--
					case when a.vl_full_price = 0 or a.vl_full_price is null then a.vl_price else a.vl_full_price end as preco_bruto,	-- algumas vendas sem full price aplica o price da venda
					case when a.vl_full_price = 0 or a.vl_full_price is null then 'N' else 'S' end as informado_preco_bruto, 
					a.vl_price as preco_unit,
					--
					coalesce(a.vl_cost, 0) as preco_custo,
					case when a.vl_cost is null or a.vl_cost = 0 then 'N' else 'S' end as informado_preco_custo,
					--
					a.qt_ordered_units + (a.qt_returned_units *-1) as quantidade,
					(a.qt_ordered_units + (a.qt_returned_units *-1)) * a.vl_price as valor
					--
				from arz_importar_vendas a
				order by a.ts_order
		) a 
) x 
where 1=1




-- fato analise cliente 
create or replace view vw_arz_analise_cliente as
select	
	y.*,
	--
	round((y.nro_pedidos_com_desconto / y.nro_pedidos_cliente) * 100) as part_desconto,
	--
	case
		 when round((y.nro_pedidos_com_desconto / y.nro_pedidos_cliente) * 100) < 33.33 then 'N'
		 when round((y.nro_pedidos_com_desconto / y.nro_pedidos_cliente) * 100) >= 33.33 then 'S' 
	end as cliente_desconto,
	--
	case
		 when round((y.nro_pedidos_com_desconto / y.nro_pedidos_cliente) * 100) < 33.33 then 0
		 when round((y.nro_pedidos_com_desconto / y.nro_pedidos_cliente) * 100) >= 33.33 then 1
	end as nro_clientes_desconto
	--
from (
		select 
			c.*,
			--
			case when c.cliente_novo = 'S' then 'Novo'
				 when c.cliente_recorrente = 'S' then 'Recorrente'
				 when c.cliente_reativado = 'S' then 'Reativado'
				 when c.cliente_perdido = 'S' then 'Perdido'
				 when c.cliente_devolucao = 'S' then 'Devolução'
				 else 'Avaliar'
			end as qualificacao,
			--
			p.dias_entre_compras,
			--
			l.ident_loja_primeira_compra,
			l.ident_loja_ultima_compra,
			l.data_primeira_compra,
			l.data_ultima_compra,
			--
			coalesce(l.retido_30_dias, 'N') as retido_30_dias,
			coalesce(l.retido_60_dias, 'N') as retido_60_dias,
			coalesce(l.retido_90_dias, 'N') as retido_90_dias,
			coalesce(l.retido_120_dias, 'N') as retido_120_dias,
			coalesce(l.retido_150_dias, 'N') as retido_150_dias,
			coalesce(l.retido_180_dias, 'N') as retido_180_dias,
			coalesce(l.retido_365_dias, 'N') as retido_365_dias,
			--
			case when l.retido_30_dias = 'S' then 1 else 0 end as nro_retido_30_dias,
			case when l.retido_60_dias = 'S' then 1 else 0 end as nro_retido_60_dias,
			case when l.retido_90_dias = 'S' then 1 else 0 end as nro_retido_90_dias,
			case when l.retido_120_dias = 'S' then 1 else 0 end as nro_retido_120_dias,
			case when l.retido_150_dias = 'S' then 1 else 0 end as nro_retido_150_dias,
			case when l.retido_180_dias = 'S' then 1 else 0 end as nro_retido_180_dias,
			case when l.retido_365_dias = 'S' then 1 else 0 end as nro_retido_365_dias,
			--
			d.nro_pedidos_com_desconto,
			d.nro_pedidos_sem_desconto
			--
		from (
				select	
					x.*,
					case when x.comprou_2024 = 'S' and x.comprou_2023 = 'N' and x.comprou_2022 = 'N' then 'S' else 'N' end as cliente_novo,
					case when x.comprou_2024 = 'S' and x.comprou_2023 = 'N' and x.comprou_2022 = 'N' then 1 else 0 end as nro_clientes_novos,
					--
					case when x.comprou_2024 = 'S' and x.comprou_2023 = 'S' then 'S' else 'N' end as cliente_recorrente,
					case when x.comprou_2024 = 'S' and x.comprou_2023 = 'S' then 1 else 0 end as nro_clientes_recorrentes,
					--
					case when x.comprou_2024 = 'S' and x.comprou_2023 = 'N' and x.comprou_2022 = 'S' then 'S' else 'N' end as cliente_reativado,
					case when x.comprou_2024 = 'S' and x.comprou_2023 = 'N' and x.comprou_2022 = 'S' then 1 else 0 end as nro_clientes_reativados,
					--
					case when x.comprou_2024 = 'N' and (x.comprou_2023 = 'S' or x.comprou_2022 = 'S') then 'S' else 'N' end as cliente_perdido,
					case when x.comprou_2024 = 'N' and (x.comprou_2023 = 'S' or x.comprou_2022 = 'S') then 1 else 0 end as nro_clientes_perdidos,
					--
					case when x.vlr_venda_total = 0 and x.vlr_devolucao_total > 0 then 'S' else 'N' end as cliente_devolucao,
					case when x.vlr_venda_total = 0 and x.vlr_devolucao_total > 0 then 1 else 0 end as nro_clientes_devolucao
					--
				from (
						select 
							a.*,
							case when a.vlr_venda_2024 > 0 then 'S' else 'N' end as ativo,
							case when a.vlr_venda_2024 > 0 then 1 else 0 end as nro_clientes_ativos,
							--
							case when a.vlr_venda_2024 > 0 then 'S' else 'N' end as comprou_2024,
							case when a.vlr_venda_2023 > 0 then 'S' else 'N' end as comprou_2023,
							case when a.vlr_venda_2022 > 0 then 'S' else 'N' end as comprou_2022
						from (
								select
									p.codigo_cliente,
									1 as nro_clientes,
									--
									sum(p.vlr_venda) as vlr_venda_total,
									sum(p.vlr_venda_bruta) as vlr_venda_bruta_total,
									sum(p.vlr_devolucao) as vlr_devolucao_total,
									--
									sum(case when extract(year from p.data_emissao) = 2024 then p.vlr_venda else 0 end) as vlr_venda_2024,
									sum(case when extract(year from p.data_emissao) = 2023 then p.vlr_venda else 0 end) as vlr_venda_2023,
									sum(case when extract(year from p.data_emissao) = 2022 then p.vlr_venda else 0 end) as vlr_venda_2022,
									--
									count(distinct p.pedido) as nro_pedidos_cliente
									--
								from vw_arz_pedido p
								where 1=1
								and p.codigo_cliente is not null 
								group by p.codigo_cliente
								order by 2 desc 
						) a
				)x 
		)c left join (select 
							p.codigo_cliente,
							round(avg(p.dias_entre_compras), 2) as dias_entre_compras
						  from vw_arz_analise_pedido p
						  where 1=1
						  group by p.codigo_cliente
						 ) p on p.codigo_cliente =  c.codigo_cliente
			   --
			   left join (
			   			  select
			   			  		a.codigo_cliente,
			   			  		max(a.ident_loja_primeira_compra) as ident_loja_primeira_compra,
			   			  		max(a.ident_loja_ultima_compra) as ident_loja_ultima_compra,
			   			  		max(a.data_primeira_compra) as data_primeira_compra,
			   			  		max(a.data_ultima_compra) as data_ultima_compra,
			   			  		--
			   			  		max(case when p.data_emissao between DATE_ADD(a.data_primeira_compra, INTERVAL 1 DAY) and DATE_ADD(a.data_primeira_compra, INTERVAL 30 DAY) then 'S' else null end) as retido_30_dias,
								max(case when p.data_emissao between DATE_ADD(a.data_primeira_compra, INTERVAL 1 DAY) and DATE_ADD(a.data_primeira_compra, INTERVAL 60 DAY) then 'S' else null end) as retido_60_dias,
								max(case when p.data_emissao between DATE_ADD(a.data_primeira_compra, INTERVAL 1 DAY) and DATE_ADD(a.data_primeira_compra, INTERVAL 90 DAY) then 'S' else null end) as retido_90_dias,
								max(case when p.data_emissao between DATE_ADD(a.data_primeira_compra, INTERVAL 1 DAY) and DATE_ADD(a.data_primeira_compra, INTERVAL 120 DAY) then 'S' else null end) as retido_120_dias,
								max(case when p.data_emissao between DATE_ADD(a.data_primeira_compra, INTERVAL 1 DAY) and DATE_ADD(a.data_primeira_compra, INTERVAL 150 DAY) then 'S' else null end) as retido_150_dias,
								max(case when p.data_emissao between DATE_ADD(a.data_primeira_compra, INTERVAL 1 DAY) and DATE_ADD(a.data_primeira_compra, INTERVAL 180 DAY) then 'S' else null end) as retido_180_dias,
								max(case when p.data_emissao between DATE_ADD(a.data_primeira_compra, INTERVAL 1 DAY) and DATE_ADD(a.data_primeira_compra, INTERVAL 365 DAY) then 'S' else null end) as retido_365_dias
			   			  from (
					   				select 
										ped.codigo_cliente,
										--
										max(case when ped.pos_pri = 1 then ped.ident_loja else null end) as ident_loja_primeira_compra,
										max(case when ped.pos_ult = 1 then ped.ident_loja else null end) as ident_loja_ultima_compra,
										--
										max(case when ped.pos_pri = 1 then ped.data_emissao else null end) as data_primeira_compra,
										max(case when ped.pos_ult = 1 then ped.data_emissao else null end) as data_ultima_compra
									from (
											select
												p.codigo_cliente,
												p.pedido,
												p.data_emissao,
												p.loja,
												p.ident_loja,
												row_number()over(partition by p.codigo_cliente order by p.data_emissao asc)  as pos_pri,
												row_number()over(partition by p.codigo_cliente order by p.data_emissao desc) as pos_ult 
											from vw_arz_pedido p
											where p.codigo_cliente <> 'Não Informado'
									) ped 
									where 1=1
									group by ped.codigo_cliente 
								) a left join vw_arz_analise_pedido p on p.codigo_cliente = a.codigo_cliente 
								group by a.codigo_cliente
						) l on l.codigo_cliente = c.codigo_cliente
				--
				left join (select
								a.codigo_cliente,
								count(distinct case when a.possui_desconto = 'S' then a.pedido end) as nro_pedidos_com_desconto,
								count(distinct case when a.possui_desconto = 'N' then a.pedido end) as nro_pedidos_sem_desconto
							from (
									select distinct 
										row_number()over(partition by p.pedido order by p.possui_desconto desc) as pos,
										p.pedido, 
										p.codigo_cliente,
										p.possui_desconto 
									from vw_arz_pedido p
								  ) a where a.pos = 1
							group by a.codigo_cliente
						 ) d on d.codigo_cliente = c.codigo_cliente
	)y
where 1=1



-- analise pedido
create or replace view vw_arz_analise_pedido as
select 
	p1.*,
	--
	p2.pedido as pedido_anterior,
	p2.data_emissao as data_pedido_anterior,
	p2.pos_pedido as pos_pedido_anterior,
	--
	datediff(p1.data_emissao, p2.data_emissao) as dias_entre_compras
from (
		select 
			a.*,
			row_number()over(partition by a.codigo_cliente order by a.data_emissao) as pos_pedido
		from (
				select distinct 
					p.codigo_cliente,
					p.pedido,
					p.data_emissao
				from vw_arz_pedido p
				where 1=1
				and p.tipo_movimento = 'Venda'
				and p.codigo_cliente <> 'Não Informado'
				order by p.codigo_cliente, p.data_emissao 
		) a 
	) p1
left join (select 
				a.*,
				row_number()over(partition by a.codigo_cliente order by a.data_emissao) as pos_pedido
			from (
					select distinct 
						p.codigo_cliente,
						p.pedido,
						p.data_emissao
					from vw_arz_pedido p
					where 1=1
					and p.tipo_movimento = 'Venda'
					and p.codigo_cliente <> 'Não Informado'
					order by p.codigo_cliente, p.data_emissao 
				) a
			) p2 on p1.codigo_cliente = p2.codigo_cliente 
			    and p1.pos_pedido = p2.pos_pedido + 1
			    and p1.pedido <> p2.pedido -- nao calcular casos de trocas do mesmo pedido com datas diferentes
		--
where 1=1
