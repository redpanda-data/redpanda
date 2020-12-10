import React from "react";

// Components
import Input from "components/shared/Inputs/Input";

const Interval = ({
  onChange,
  defaultValue = 1000,
}: {
  onChange: (event: any) => void;
  defaultValue: number;
}) => {
  return (
    <>
      <Input
        type="number"
        step="500"
        min="500"
        max="60000"
        onChange={onChange}
        defaultValue={defaultValue}
      />{" "}
      ms
    </>
  );
};

export default Interval;
