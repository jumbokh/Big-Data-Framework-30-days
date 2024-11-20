# echo "some testing text" | python <path/to/mapper.py> | sort | python <path/to/reducer.py>
echo "some testing text" | python mapper.py | sort | python reducer.py
