WITH films_with_rating AS (
    select 
    film_id,
    title,
    release_date,
    price,
    rating,
    user_rating,
    CASE
        when user_rating >= 4.5 Then 'Excellent'
        when user_rating >= 3.5 Then 'Good'
        when user_rating >= 2.5 Then 'Average'
        when user_rating >= 1.5 Then 'Poor'
        else 'Very Poor'
        END AS rating_category
    from {{ref('films')}}
),

films_with_actors AS (
select 
    f.film_id,
    f.title,
    string_agg(a.actor_name, ',') AS actors
from {{ref('films')}} f
left join {{ref('film_actors')}} fa on f.film_id = fa.film_id
left join {{ref('actors')}} a on fa.actor_id = a.actor_id
group by f.film_id, f.title
)

select 
    fwf.*,
    fwa.actors
from films_with_rating fwf
left join films_with_actors fwa on fwf.film_id = fwa.film_id