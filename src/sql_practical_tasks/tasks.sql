-- TASK 1
with total_number_of_books_for_authors as (select author_id, count(book_id) as total_books
                                           from books
                                           group by author_id)

select a.name
from authors a
         inner join total_number_of_books_for_authors tnoffa
                    on tnoffa.author_id = a.author_id
where tnoffa.total_books > 3;

-- TASK 2
with books_titles as (select *
                      from books b
                      where b.title ~* '(?:^|\W)the(?:\W|$)')

select bt.title, a.name, bt.published_date, g.genre_name
from books_titles bt
         inner join authors a
                    on bt.author_id = a.author_id
         inner join genres g
                    on bt.genre_id = g.genre_id;

-- TASK 3
select book_id,
       title,
       author_id,
       published_date,
       genre_id,
       price,
       rank() over (partition by genre_id order by price desc)
from books;

-- TASK 4
create or replace procedure sp_bulk_update_book_prices_by_genre(p_genre_id integer, p_percentage_change numeric(5, 2))
    language plpgsql
as
$$
declare
    v_updated_count integer;
begin
    update books set price = price * (1 + p_percentage_change / 100) where genre_id = p_genre_id;
    get diagnostics v_updated_count = row_count;
    raise notice 'Number of books updated: %', v_updated_count;
end;
$$;

CALL sp_bulk_update_book_prices_by_genre(3, 5.00);

-- TASK 5
create or replace procedure sp_update_customer_join_date()
    language plpgsql
as
$$
begin
    update customers
    set join_date = subquery.sale_date
    FROM (select s.customer_id,
                 MIN(s.sale_date) as sale_date
          from sales s
          group by s.customer_id) subquery
    WHERE customers.customer_id = subquery.customer_id
      AND (customers.join_date is NULL or customers.join_date < subquery.sale_date);
end;
$$;
CALL sp_update_customer_join_date();

-- TASK 6
create or replace function fn_avg_price_by_genre(p_genre_id integer) returns numeric(10, 2) as
$$
declare
    avg_price numeric(10, 2);
begin
    select avg(price)
    into avg_price
    from books
    where genre_id = p_genre_id;
    return avg_price;
end;
$$ language plpgsql;

SELECT fn_avg_price_by_genre(1);

-- TASK 7
create or replace function fn_get_top_n_books_by_genre(p_genre_id integer, p_top_n integer)
    returns table
            (
                book_id     integer,
                title       varchar(255),
                author_id   integer,
                total_sales bigint
            )
as
$$
begin
    return query
        select b.book_id,
               b.title,
               b.author_id,
               coalesce(sum(s.quantity), 0) as total_sales
        from books b
                 left join sales s
                           on b.book_id = s.book_id
        where b.genre_id = p_genre_id
        group by b.book_id, b.title, b.author_id, b.genre_id
        order by total_sales desc
        limit p_top_n;
end;
$$ language plpgsql;

SELECT *
FROM fn_get_top_n_books_by_genre(1, 5);

-- TASK 8
create or replace function log_sensitive_data_changes()
    returns trigger as
$$
begin
    if old.first_name is distinct from new.first_name then
        insert into CustomersLog (column_name, old_value, new_value, changed_by)
        values ('first_name', old.first_name, new.first_name, current_user);
    end if;

    if old.last_name is distinct from new.last_name then
        insert into CustomersLog (column_name, old_value, new_value, changed_by)
        values ('last_name', old.last_name, new.last_name, current_user);
    end if;

    if old.email is distinct from new.email then
        insert into CustomersLog (column_name, old_value, new_value, changed_by)
        values ('email', old.email, new.email, current_user);
    end if;

    return new;
end;
$$ language plpgsql;

create trigger tr_log_sensitive_data_changes
    after update
    on customers
    for each row
execute function log_sensitive_data_changes();

-- TASK 9
create or replace function adjust_book_price()
    returns trigger as
$$
declare
    v_total_quantity integer;
    v_threshold      integer := 5;
begin
    select coalesce(sum(quantity), 0)
    into v_total_quantity
    from sales
    where book_id = new.book_id;

    if v_total_quantity >= v_threshold then
        update books
        set price = price * 1.10
        where book_id = new.book_id;
    end if;

    return new;
end;
$$ language plpgsql;

create trigger tr_adjust_book_price
    after insert
    on sales
    for each row
execute function adjust_book_price();


-- TASK 10
CREATE TABLE SalesArchive
(
    sale_id     SERIAL PRIMARY KEY,
    book_id     INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    quantity    INTEGER NOT NULL,
    sale_date   DATE    NOT NULL,
    FOREIGN KEY (book_id) REFERENCES Books (book_id),
    FOREIGN KEY (customer_id) REFERENCES Customers (customer_id)
);

create or replace procedure sp_archive_old_sales(p_cutoff_date date)
    language plpgsql
as
$$
declare
    sales_cursor cursor for
        select sale_id, book_id, customer_id, quantity, sale_date
        from sales
        where sale_date < p_cutoff_date;
    sales_record record;
begin
    open sales_cursor;
    loop
        fetch sales_cursor into sales_record;
        exit when not found;

        insert into SalesArchive (sale_id, book_id, customer_id, quantity, sale_date)
        values (sales_record.sale_id, sales_record.book_id, sales_record.customer_id, sales_record.quantity,
                sales_record.sale_date);

        delete
        from sales
        where sale_id = sales_record.sale_id;
    end loop;

    close sales_cursor;

    raise notice 'archived and deleted sales records older than %', p_cutoff_date;
end;
$$;
