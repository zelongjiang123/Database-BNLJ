#include "join.hpp"

#include <array>
#include <cstdint>
#include <vector>
#include<iostream>
using namespace std;

JoinAlgorithm getJoinAlgorithm() {
	// TODO: Return your chosen algorithm.
	return JOIN_ALGORITHM_BNLJ;
	// return JOIN_ALGORITHM_SMJ;
	// return JOIN_ALGORITHM_HJ;
	//throw std::runtime_error("not implemented: getJoinAlgorithm");
};

int join(File& file, int numPagesR, int numPagesS, char* buffer,
	int numFrames) {
	// TODO: Implement your chosen algorithm.
	// Currently, this function performs a nested loop join in memory. While it
	// will pass the correctness and I/O cost tests, it ignores the provided
	// buffer and instead reads table data into vectors. It will not satisfy the
	// constraints on peak heap memory usage for large input tables. You should
	// delete this code and replace it with your disk-based join algorithm
	// implementation.

	int pageIndexR = 0;
	int pageIndexS = pageIndexR + numPagesR;
	int pageIndexOut = pageIndexS + numPagesS;
	const int tuples_per_page = 512;
	const int spaceforR = numFrames - 2; // the pages for R in the buffer
	int outer_loops = numPagesR / spaceforR; // store the times that the outer loop need to iterate

	int numTuplesOut = 0; // the number of Tuples in the output
	Tuple* buffer_out = (Tuple*)buffer + (spaceforR + 1) * tuples_per_page; // the pointer to the output buffer
	int index = 0; // the index of the output tuples
	for (int i = 0; i < outer_loops; i++)
	{
		Tuple* buffer_R = (Tuple*)buffer;
		//cout << "read page index R " << pageIndexR << endl;
		file.read(buffer_R, pageIndexR, spaceforR); // read B-2 pages from R
		pageIndexR += spaceforR;
		pageIndexS = numPagesR; // reset S page index
		for (int j = 0; j < numPagesS; j++)
		{
			Tuple* buffer_S = buffer_R + spaceforR * tuples_per_page;
			//cout << "read page index S " << pageIndexS << endl;
			file.read(buffer_S, pageIndexS++, 1); // read 1 page from S
			for (int k = 0; k < tuples_per_page; k++) // iterate over tuples in page of S
			{
				for (int l = 0; l < tuples_per_page * spaceforR; l++) // iterate over tuples in pages of R
				{
					if (buffer_R[l].first == buffer_S[k].first)
					{
						//cout << buffer_R[l].first << endl;
						buffer_out[index].first = buffer_R[l].second;
						buffer_out[index++].second = buffer_S[k].second;
						numTuplesOut++;
						if (index == tuples_per_page) // the output buffer is full and we can write it to file
						{
							file.write((void*)buffer_out, pageIndexOut++, 1);
							index = 0;
						}
					}
				}
			}

			//cout << "this tuple has data " << tupleS->first << " " << tupleS->second << endl;
		}
	}
	/*
	* need to consider the case that R still has some pages left
	*/
	pageIndexS = numPagesR; // reset S page index
	numPagesR -= outer_loops * spaceforR;
	if (numPagesR != 0)
	{
		Tuple* buffer_R = (Tuple*)buffer;
		//cout << "read page index R " << pageIndexR << endl;
		file.read(buffer_R, pageIndexR, numPagesR); // read all pages left in R
		for (int j = 0; j < numPagesS; j++)
		{
			Tuple* buffer_S = buffer_R + spaceforR * tuples_per_page;
			//cout << "read page index S " << pageIndexS << endl;
			file.read(buffer_S, pageIndexS++, 1); // read 1 page from S
			for (int k = 0; k < tuples_per_page; k++) // iterate over tuples in page of S
			{
				for (int l = 0; l < tuples_per_page * numPagesR; l++) // iterate over tuples in pages of R
				{
					if (buffer_R[l].first == buffer_S[k].first)
					{
						//cout << buffer_R[l].first << endl;
						buffer_out[index].first = buffer_R[l].second;
						buffer_out[index++].second = buffer_S[k].second;
						numTuplesOut++;
						if (index == tuples_per_page) // the output buffer is full and we can write it to file
						{
							file.write((void*)buffer_out, pageIndexOut++, 1);
							index = 0;
						}
					}
				}
			}
			//cout << "this tuple has data " << tupleS->first << " " << tupleS->second << endl;
		}
	}

	if (index != 0)
	{
		// need to set the rest byte of the output buffer to 0 
		//for (int i = index; i < tuples_per_page; i++)
		  //  buffer_out[i] = (Tuple)0;
		file.write((void*)buffer_out, pageIndexOut++, 1);
	}
	//cout << "number of tuples out is " << numTuplesOut << endl;

	return numTuplesOut;

}
