import React from "react";

// Components
// import Input from "components/shared/Inputs/Input";
import Select from "components/shared/Inputs/Select";

const TimeWindow = ({
  onChange,
  defaultValue,
  value,
}: {
  onChange: (event: any) => void;
  defaultValue: number;
  value: number;
}) => {

  const options = [
    { value: 1000*10,       label: '10 seconds' },
    { value: 1000*30,       label: '30 seconds' },
    { value: 1000*60,       label: '60 seconds' },
    { break: true },
    { value: 1000*60*5,     label: '5 minutes' },
    { value: 1000*60*15,    label: '15 minutes' },
    { value: 1000*60*30,    label: '30 minutes' },
    { value: 1000*60*60,    label: '60 minutes' },
    { break: true },
    { value: 1000*60*60*2,  label: '2 hours' },
    { value: 1000*60*60*6,  label: '6 hours' },
    { value: 1000*60*60*12, label: '12 hours' },
    { value: 1000*60*60*24, label: '24 hours' },
  ]

  return (
    <>
      {/* <Input
        type="number"
        step="0.25"
        min="6"
        max={60*24}
        onChange={onChange}
        defaultValue={defaultValue}
      />{" "}
      mins */}
      <Select onChange={onChange} defaultValue={value}>
        {options.map((option,i)=> option.break ? (
          <option key={i} disabled style={{ borderBottom: '1px dotted #000' }}>–––</option>
        ):(
          <option 
            key={i}
            value={option.value} 
          >{option.label}</option>
        ))}
      </Select>
    </>
  );
};

export default TimeWindow;
